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


def _enter_alt_screen() -> bool:
    """Enter the terminal's alternate screen buffer. Returns True if we did.

    Kept as a single entry/exit at the outermost mode boundary so that
    transitions between summary and detail views do not flash the user's
    shell — Rich's Live runs in screen=False mode inside this buffer.
    """
    if sys.platform == 'win32':
        return False  # Rich handles alt screen on Windows
    sys.stdout.write('\033[?1049h\033[H')
    sys.stdout.flush()
    return True


def _leave_alt_screen(entered: bool) -> None:
    if entered:
        sys.stdout.write('\033[?1049l')
        sys.stdout.flush()


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
