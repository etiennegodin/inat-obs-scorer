import argparse
import logging
import sys
from argparse import Namespace
from pathlib import Path

from .app import ApplicationService, Dependencies
from .exceptions import InatPipelineError
from .model.registery import (
    CLASSIFIER_REGISTRY,
    ENCODER_REGISTRY,
    IMPUTER_REGISTRY,
    REDUCER_REGISTRY,
    SCALER_REGISTRY,
)
from .utils.logger import init_logger


def ingest_cmd(args: Namespace, app: ApplicationService):
    try:
        app.ingest(api_limit=args.api_limit)
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1


def features_cmd(args: Namespace, app: ApplicationService):
    try:
        app.features(limit=args.limit)
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1


def train_cmd(args: Namespace, app: ApplicationService):
    try:
        result = app.train(args)
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1

    print("\n✓ Model run passed!")
    print(f"  Test ROC-AUC: {result['test_metrics']['test_roc_auc']:.4f}")


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line argument parser.

    Returns:
        Configured ArgumentParser with subcommands for publish, install, and scan.
    """
    parser = argparse.ArgumentParser(
        prog="inat_pipeline", description="Data pipeline for inaturalist observations"
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(
        title="commands", description="Available commands"
    )

    # Ingest command
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingests data sources, runs api queries, saves to db and stage data",
    )
    ingest_parser.add_argument("--api_limit", default=None)
    ingest_parser.set_defaults(func=ingest_cmd)

    # Process command
    process_parser = subparsers.add_parser("features", help="Creates features suite")
    process_parser.add_argument("--limit", "-l", default=None)
    process_parser.set_defaults(func=features_cmd)

    # Process command
    train_parser = subparsers.add_parser("train", help="NotImplemented")
    train_parser.add_argument(
        "--classifier", default="logistic", choices=CLASSIFIER_REGISTRY
    )
    train_parser.add_argument("--reducer", default="none", choices=REDUCER_REGISTRY)
    train_parser.add_argument("--scaler", default="standard", choices=SCALER_REGISTRY)
    train_parser.add_argument("--encoder", default="onehot", choices=ENCODER_REGISTRY)
    train_parser.add_argument("--imputer", default="median", choices=IMPUTER_REGISTRY)
    train_parser.add_argument("--n_trials", "-n", default=10, type=int)
    train_parser.add_argument("--cv_folds", default=5)

    train_parser.add_argument(
        "--test", "-t", default=False, action="store_true", help="Run a quick test"
    )
    train_parser.set_defaults(func=train_cmd)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    logger = init_logger(Path.cwd() / "log.log", logging.INFO)

    # Create dependencies
    try:
        deps = Dependencies(
            logger=logger,
            project_root=Path(__file__).parents[2],
            package_root=Path(__file__).parents[0],
        )
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
