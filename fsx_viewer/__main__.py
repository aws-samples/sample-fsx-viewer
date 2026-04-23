"""Entry point for fsx-viewer."""

import logging
import os
import sys
import signal

import boto3

from . import __version__
from .cli import parse_args, Config
from .model import Store, DetailStore
from .aws_client import FSxClient, CloudWatchClient, StaticPricingProvider, create_session
from .controller import Controller, DetailController, FileSystemNotFoundError, Config as ControllerConfig
from .ui import UI, DetailUI, Style


def setup_logging():
    """Configure logging based on environment variable."""
    log_level = logging.WARNING  # Default to WARNING
    if os.getenv('FSX_VIEWER_DEBUG'):
        log_level = logging.DEBUG
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """Main entry point."""
    # Setup logging first
    setup_logging()
    
    # Parse command-line arguments
    try:
        config = parse_args()
    except SystemExit:
        return 1
    
    # Handle version flag
    if config.show_version:
        print(f"fsx-viewer {__version__}")
        return 0
    
    # Initialize AWS clients with shared session for efficiency
    try:
        session = create_session(region=config.region, profile=config.profile)
        fsx_client = FSxClient(region=config.region, session=session)
        cw_client = CloudWatchClient(region=config.region, session=session)
        pricing = StaticPricingProvider(region=config.region)
    except Exception as e:
        print(f"Error initializing AWS clients: {e}", file=sys.stderr)
        return 1
    
    # Create controller config
    controller_config = ControllerConfig(
        file_system_type=config.file_system_type,
        name_filter=config.name_filter,
        refresh_interval=config.refresh_interval,
        metric_interval=config.metric_interval,
    )
    
    # Parse style
    style = Style.parse(config.style)
    
    # Validate mutually exclusive options
    if config.file_system_id and config.file_system_type:
        print(
            "Invalid usage: --type and --file-system-id cannot be used together.\n"
            "Use --type to filter the summary view OR --file-system-id for detailed view.",
            file=sys.stderr
        )
        return 1
    
    # Determine view mode
    if config.file_system_id:
        return _run_detail_mode(
            config=config,
            fsx_client=fsx_client,
            cw_client=cw_client,
            pricing=pricing,
            controller_config=controller_config,
            style=style,
        )
    else:
        return _run_summary_mode(
            config=config,
            fsx_client=fsx_client,
            cw_client=cw_client,
            pricing=pricing,
            controller_config=controller_config,
            style=style,
        )


def _enable_win_vt() -> bool:
    """Enable Virtual Terminal Processing on Windows so ANSI escapes work.

    Returns True if VT mode is active (either already enabled or we enabled it).
    Returns False if we couldn't enable it (legacy console).
    """
    if sys.platform != 'win32':
        return True  # Unix always supports ANSI
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        STD_OUTPUT_HANDLE = -11
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        handle = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        mode = ctypes.c_ulong()
        kernel32.GetConsoleMode(handle, ctypes.byref(mode))
        kernel32.SetConsoleMode(handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
        return True
    except Exception:
        return False


# Whether ANSI escapes work in this terminal (set once at startup)
_vt_enabled = _enable_win_vt()


def _enter_alt_screen() -> bool:
    """Enter the terminal's alternate screen buffer."""
    if _vt_enabled:
        sys.stdout.write('\033[?1049h\033[H')
        sys.stdout.flush()
        return True
    elif sys.platform == 'win32':
        import os; os.system('cls')
        return False
    return False


def _leave_alt_screen(entered: bool) -> None:
    if entered:
        sys.stdout.write('\033[?1049l')
        sys.stdout.flush()


# Remember the last-used Instance Connect Endpoint ID for the session so the
# user doesn't have to retype it for every SSH.
_last_eice_id: str | None = None


def _ssh_to_fsx(fs_id: str, management_ip: str, entered_alt: bool) -> None:
    """Stay in the alt-screen but clear it, prompt for an Instance Connect
    Endpoint ID, and SSH to the FSx ONTAP management endpoint via
    ec2-instance-connect.

    Returns when the SSH session exits. We deliberately do *not* leave the
    alt-screen, so the user's original shell is never exposed and the SSH
    prompt plus session run on a clean screen. On return, we clear the
    alt-screen again so the caller's next render starts fresh.
    """
    global _last_eice_id
    import re
    import subprocess

    def _clear() -> None:
        if _vt_enabled:
            sys.stdout.write('\033[2J\033[H')
            sys.stdout.flush()
        elif sys.platform == 'win32':
            import os as _os; _os.system('cls')

    _clear()
    try:
        print(f"SSH to {fs_id} (management endpoint {management_ip})\n")
        default_suffix = f" [{_last_eice_id}]" if _last_eice_id else ""
        raw = input(f"Instance Connect Endpoint ID (eice-...){default_suffix}: ").strip()
        eice_id = raw or (_last_eice_id or "")
        if not re.fullmatch(r"eice-[0-9a-f]+", eice_id):
            print(f"Invalid Instance Connect Endpoint ID: {eice_id!r}")
            input("Press Enter to return to fsx-viewer...")
            return
        _last_eice_id = eice_id

        proxy = (
            f"aws ec2-instance-connect open-tunnel "
            f"--instance-connect-endpoint-id {eice_id} "
            f"--private-ip-address {management_ip}"
        )
        cmd = [
            "ssh",
            f"fsxadmin@{management_ip}",
            "-o", f"ProxyCommand={proxy}",
        ]
        print(f"\n$ {' '.join(cmd)}\n", flush=True)
        try:
            subprocess.call(cmd)
        except FileNotFoundError:
            print("Error: 'ssh' is not on PATH. Install OpenSSH and try again.")
            input("Press Enter to return to fsx-viewer...")
    finally:
        # Clear the alt-screen again so the caller's next TUI render is clean.
        _clear()


def _run_summary_mode(
    config: Config,
    fsx_client: FSxClient,
    cw_client: CloudWatchClient,
    pricing: StaticPricingProvider,
    controller_config: ControllerConfig,
    style: Style,
) -> int:
    """Run the summary view mode (default).

    Holds the alternate screen buffer once for the whole session (non-Windows);
    Rich's Live is configured to render inline (screen=False) so switching
    between summary and detail views does not flash the user's shell.
    """
    entered_alt = _enter_alt_screen()
    try:
        while True:
            # Create store and controller
            store = Store()
            controller = Controller(
                fsx_client=fsx_client,
                cw_client=cw_client,
                store=store,
                pricing=pricing,
                config=controller_config,
            )

            # Create UI
            ui = UI(
                store=store,
                sort=config.sort,
                style=style,
                disable_pricing=config.disable_pricing,
                region=config.region,
            )

            # Set up signal handlers for graceful shutdown
            def signal_handler(sig, frame):
                ui.stop()
                controller.stop()
                _leave_alt_screen(entered_alt)
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            try:
                controller.start()
                ui.run()
            except KeyboardInterrupt:
                controller.stop()
                return 0
            finally:
                controller.stop()

            # Check if user pressed 'c' to SSH to an ONTAP file system.
            ssh_fs_id = ui.get_ssh_fs_id()
            if ssh_fs_id:
                fs_obj = store.get(ssh_fs_id)
                if fs_obj is not None and fs_obj.management_ip:
                    _ssh_to_fsx(fs_obj.id, fs_obj.management_ip, entered_alt)
                # Loop again to re-render a fresh summary view.
                continue

            # Check if user selected a file system to view details
            selected_fs_id = ui.get_selected_fs_id()
            if selected_fs_id:
                result = _run_detail_mode_for_fs(
                    file_system_id=selected_fs_id,
                    fsx_client=fsx_client,
                    cw_client=cw_client,
                    pricing=pricing,
                    controller_config=controller_config,
                    style=style,
                    disable_pricing=config.disable_pricing,
                    sort=config.sort,
                    region=config.region,
                )
                if result != 0:
                    return result
            else:
                return 0
    finally:
        _leave_alt_screen(entered_alt)


def _run_detail_mode_for_fs(
    file_system_id: str,
    fsx_client: FSxClient,
    cw_client: CloudWatchClient,
    pricing: StaticPricingProvider,
    controller_config: ControllerConfig,
    style: Style,
    disable_pricing: bool,
    sort: str = "name=asc",
    region: str = "us-east-1",
) -> int:
    """Run detail view for a specific file system (called from summary view)."""
    store = DetailStore()
    controller = DetailController(
        fsx_client=fsx_client,
        cw_client=cw_client,
        store=store,
        pricing=pricing,
        file_system_id=file_system_id,
        config=controller_config,
    )

    ui = DetailUI(
        store=store,
        style=style,
        disable_pricing=disable_pricing,
        sort=sort,
        name_filter=controller_config.name_filter,
        region=region,
    )

    def signal_handler(sig, frame):
        ui.stop()
        controller.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        controller.start()
        ui.run()
    except FileSystemNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        pass
    finally:
        controller.stop()

    return 0


def _run_detail_mode(
    config: Config,
    fsx_client: FSxClient,
    cw_client: CloudWatchClient,
    pricing: StaticPricingProvider,
    controller_config: ControllerConfig,
    style: Style,
) -> int:
    """Run the detail view mode for a specific file system."""
    entered_alt = _enter_alt_screen()
    try:
        return _run_detail_mode_for_fs(
            file_system_id=config.file_system_id,
            fsx_client=fsx_client,
            cw_client=cw_client,
            pricing=pricing,
            controller_config=controller_config,
            style=style,
            disable_pricing=config.disable_pricing,
            sort=config.sort,
            region=config.region,
        )
    finally:
        _leave_alt_screen(entered_alt)


if __name__ == "__main__":
    sys.exit(main())
