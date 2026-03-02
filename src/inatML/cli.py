import argparse
import logging
import sys
from pathlib import Path

from .utils.logger import init_logger


def ingest_cmd(args):
    pass


def process_cmd(args):
    pass


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line argument parser.

    Returns:
        Configured ArgumentParser with subcommands for publish, install, and scan.
    """
    parser = argparse.ArgumentParser(
        prog="nukekit", description="Nuke asset management system"
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(
        title="commands", description="Available commands"
    )

    # Ingest command
    publish_parser = subparsers.add_parser("ingest", help="NotImplemented")

    publish_parser.set_defaults(func=ingest_cmd)

    # Ingest command
    process_parser = subparsers.add_parser("process", help="NotImplemented")

    process_parser.set_defaults(func=process_cmd)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    logger = init_logger(
        Path.cwd(), level=logging.DEBUG if args.verbose else logging.INFO
    )

    try:
        # Execute command
        if hasattr(args, "func"):
            exit_code = args.func(args)
            sys.exit(exit_code)
        else:
            parser.print_help()
            sys.exit(0)
    except KeyboardInterrupt:
        print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        logger.exception("Unexpected error")
        print(f"[red]Unexpected error: {e}[/red]")
        sys.exit(1)
