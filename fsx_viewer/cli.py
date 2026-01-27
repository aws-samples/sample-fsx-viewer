"""Command-line argument parsing for FSx Viewer."""

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Config:
    """Configuration from CLI args, env vars, and config file."""

    region: str
    profile: Optional[str] = None
    file_system_id: Optional[str] = None  # For detail view mode
    file_system_type: Optional[str] = None
    name_filter: Optional[str] = None
    sort: str = "creation=dsc"
    refresh_interval: int = 300  # 5 minutes
    metric_interval: int = 60  # 60 seconds
    disable_pricing: bool = False
    style: str = "green,yellow,red"
    show_version: bool = False


def load_config_file() -> dict:
    """Load configuration from ~/.fsx-viewer config file."""
    config_path = Path.home() / ".fsx-viewer"
    config = {}

    if not config_path.exists():
        return config

    try:
        with open(config_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip().replace("-", "_")
                    value = value.strip()
                    config[key] = value
    except Exception:
        pass

    return config


def parse_args(args: Optional[list] = None) -> Config:
    """Parse command-line arguments with config file and env var fallbacks.

    Precedence: CLI args > env vars > config file > defaults
    """
    # Load config file first (lowest precedence)
    file_config = load_config_file()

    # Get env vars (medium precedence)
    env_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    env_profile = os.environ.get("AWS_PROFILE")

    # Set up argument parser
    parser = argparse.ArgumentParser(
        prog="fsx-viewer",
        description="Terminal-based FSx file system monitoring tool",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="Show version and exit",
    )

    parser.add_argument(
        "-r",
        "--region",
        type=str,
        default=None,
        help="AWS region (default: from env or config)",
    )

    parser.add_argument(
        "-p",
        "--profile",
        type=str,
        default=None,
        help="AWS profile name (default: from env or config)",
    )

    parser.add_argument(
        "-f",
        "--file-system-id",
        type=str,
        default=None,
        help="Show detail view for a specific file system (e.g., fs-0123456789abcdef0)",
    )

    parser.add_argument(
        "-t",
        "--type",
        type=str,
        choices=["LUSTRE", "WINDOWS", "ONTAP", "OPENZFS"],
        default=None,
        help="Filter by file system type (summary view only)",
    )

    parser.add_argument(
        "-n",
        "--name-filter",
        type=str,
        default=None,
        help="Filter by name (substring match)",
    )

    parser.add_argument(
        "-s",
        "--sort",
        type=str,
        default=None,
        help="Sort field and order (e.g., 'capacity=dsc', 'name=asc')",
    )

    parser.add_argument(
        "--refresh-interval",
        type=int,
        default=None,
        help="Seconds between file system refreshes (default: 300)",
    )

    parser.add_argument(
        "--metric-interval",
        type=int,
        default=None,
        help="Seconds between metric refreshes (default: 60)",
    )

    parser.add_argument(
        "--disable-pricing",
        action="store_true",
        default=None,
        help="Disable pricing display",
    )

    parser.add_argument(
        "--style",
        type=str,
        default=None,
        help="Color style (comma-separated: good,ok,bad)",
    )

    parsed = parser.parse_args(args)

    # Build config with precedence: CLI > env > file > defaults
    def get_value(cli_val, env_val, file_key, default):
        if cli_val is not None:
            return cli_val
        if env_val is not None:
            return env_val
        if file_key in file_config:
            return file_config[file_key]
        return default

    # Region is required
    region = get_value(parsed.region, env_region, "region", None)
    if not region and not parsed.version:
        parser.error(
            "Invalid usage: Region is required.\nSet via --region, AWS_REGION env var, or config file (~/.fsx-viewer)."
        )

    return Config(
        region=region or "",
        profile=get_value(parsed.profile, env_profile, "profile", None),
        file_system_id=parsed.file_system_id,
        file_system_type=get_value(parsed.type, None, "file_system_type", None),
        name_filter=get_value(parsed.name_filter, None, "name_filter", None),
        sort=get_value(parsed.sort, None, "sort", "creation=dsc"),
        refresh_interval=int(
            get_value(parsed.refresh_interval, None, "refresh_interval", 300)
        ),
        metric_interval=int(
            get_value(parsed.metric_interval, None, "metric_interval", 60)
        ),
        disable_pricing=parsed.disable_pricing
        if parsed.disable_pricing
        else file_config.get("disable_pricing", "false").lower() == "true",
        style=get_value(parsed.style, None, "style", "green,yellow,red"),
        show_version=parsed.version,
    )
