"""Entry point for fsx-viewer."""

import sys
import signal

import boto3

from . import __version__
from .cli import parse_args, Config
from .model import Store, DetailStore
from .aws_client import FSxClient, CloudWatchClient, StaticPricingProvider, create_session
from .controller import Controller, DetailController, FileSystemNotFoundError, Config as ControllerConfig
from .ui import UI, DetailUI, Style


def main():
    """Main entry point."""
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


def _run_summary_mode(
    config: Config,
    fsx_client: FSxClient,
    cw_client: CloudWatchClient,
    pricing: StaticPricingProvider,
    controller_config: ControllerConfig,
    style: Style,
) -> int:
    """Run the summary view mode (default)."""
    # Switch to alternate screen buffer once for the entire session
    sys.stdout.write('\033[?1049h')
    sys.stdout.flush()
    
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
            )
            
            # Set up signal handlers for graceful shutdown
            def signal_handler(sig, frame):
                ui.stop()
                controller.stop()
                # Restore screen before exit
                sys.stdout.write('\033[?1049l')
                sys.stdout.flush()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Start controller and UI (don't let UI manage screen)
            try:
                controller.start()
                ui.run(manage_screen=False)
            except KeyboardInterrupt:
                controller.stop()
                return 0
            finally:
                controller.stop()
            
            # Check if user selected a file system to view details
            selected_fs_id = ui.get_selected_fs_id()
            if selected_fs_id:
                # Run detail view for selected file system (don't manage screen)
                result = _run_detail_mode_for_fs(
                    file_system_id=selected_fs_id,
                    fsx_client=fsx_client,
                    cw_client=cw_client,
                    pricing=pricing,
                    controller_config=controller_config,
                    style=style,
                    disable_pricing=config.disable_pricing,
                    sort=config.sort,
                    manage_screen=False,
                )
                # After detail view exits, loop back to summary view
                if result != 0:
                    return result
            else:
                # User quit without selecting
                return 0
    finally:
        # Restore original screen buffer
        sys.stdout.write('\033[?1049l')
        sys.stdout.flush()


def _run_detail_mode_for_fs(
    file_system_id: str,
    fsx_client: FSxClient,
    cw_client: CloudWatchClient,
    pricing: StaticPricingProvider,
    controller_config: ControllerConfig,
    style: Style,
    disable_pricing: bool,
    sort: str = "name=asc",
    manage_screen: bool = True,
) -> int:
    """Run detail view for a specific file system (called from summary view)."""
    # Create detail store and controller
    store = DetailStore()
    controller = DetailController(
        fsx_client=fsx_client,
        cw_client=cw_client,
        store=store,
        pricing=pricing,
        file_system_id=file_system_id,
        config=controller_config,
    )
    
    # Create detail UI
    ui = DetailUI(
        store=store,
        style=style,
        disable_pricing=disable_pricing,
        sort=sort,
    )
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        ui.stop()
        controller.stop()
        if manage_screen:
            sys.stdout.write('\033[?1049l')
            sys.stdout.flush()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start controller and UI
    try:
        controller.start()
        ui.run(manage_screen=manage_screen)
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
    # Create detail store and controller
    store = DetailStore()
    controller = DetailController(
        fsx_client=fsx_client,
        cw_client=cw_client,
        store=store,
        pricing=pricing,
        file_system_id=config.file_system_id,
        config=controller_config,
    )
    
    # Create detail UI
    ui = DetailUI(
        store=store,
        style=style,
        disable_pricing=config.disable_pricing,
        sort=config.sort,
    )
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        ui.stop()
        controller.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start controller and UI
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


if __name__ == "__main__":
    sys.exit(main())
