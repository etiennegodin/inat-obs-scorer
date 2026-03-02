import argparse
import logging
import sys
from argparse import Namespace
from pathlib import Path

from .app import ApplicationService, Dependencies
from .utils.logger import init_logger


def ingest_cmd(args: Namespace, app: ApplicationService):
    try:
        # app.ingest_download_data()
        app.ingest_inat_api_data()

    except Exception as e:
        print(e)


def process_cmd(args):
    pass


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line argument parser.

    Returns:
        Configured ArgumentParser with subcommands for publish, install, and scan.
    """
    parser = argparse.ArgumentParser(prog="inatML", description="Inat ML features")

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(
        title="commands", description="Available commands"
    )

    # Ingest command
    publish_parser = subparsers.add_parser("ingest", help="NotImplemented")

    publish_parser.set_defaults(func=ingest_cmd)

    # Process command
    process_parser = subparsers.add_parser("process", help="NotImplemented")

    process_parser.set_defaults(func=process_cmd)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    logger = init_logger(Path.cwd() / "log.log", logging.INFO)

    # Create dependencies
    try:
        deps = Dependencies(logger=logger, root=Path(__file__).parents[2])
        print(deps.root)
    except Exception as e:
        print(f"[red]Configuration error: {e}[/red]")
        sys.exit(1)

    # Create application service
    app = ApplicationService(deps)

    try:
        # Execute command
        if hasattr(args, "func"):
            exit_code = args.func(args, app)
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
