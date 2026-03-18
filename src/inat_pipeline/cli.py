import argparse
import logging
import random
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
from .utils import get_git_branch, get_git_hash, init_logger


def ingest_local_cmd(args: Namespace, app: ApplicationService):
    try:
        app.ingest_local(args)
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1


def ingest_api_cmd(args: Namespace, app: ApplicationService):
    try:
        app.ingest_api(args)
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1


def features_cmd(args: Namespace, app: ApplicationService):
    try:
        app.features()
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1
    except Exception as e:
        print(f"[red]✗ Unexpected error: {e}[/red]")
        return 1


def train_cmd(args: Namespace, app: ApplicationService):
    if args.gpu:
        raise NotImplementedError("Not implemented")

    try:
        result = app.train(args)
    except InatPipelineError as e:
        print(f"[red]✗ {e}[/red]")
        return 1

    print("\n✓ Model run passed!")
    print(f"  Test ROC-AUC: {result['test_metrics']['test/test_roc_auc']:.4f}")


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line argument parser.

    Returns:
        Configured ArgumentParser with subcommands for publish, install, and scan.
    """
    parser = argparse.ArgumentParser(
        prog="inat_pipeline", description="Data pipeline for inaturalist observations"
    )

    subparsers = parser.add_subparsers(
        title="commands", description="Available commands"
    )

    # Ingest command
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingests data sources [local, api]",
    )

    ingest_subparsers = ingest_parser.add_subparsers(
        title="module", description="Available modules"
    )

    ingest_local_parser = ingest_subparsers.add_parser(
        "local",
        help="Ingests data from prior downloads",
    )
    ingest_local_parser.set_defaults(func=ingest_local_cmd)

    ingest_api_parser = ingest_subparsers.add_parser(
        "api",
        help="Ingest data from inaturalist's api",
    )
    ingest_api_parser.add_argument(
        "--rate", "-r", default=30, type=int, help="Requests per min"
    )
    ingest_api_parser.add_argument(
        "--ignore_not_found",
        "-i",
        action="store_true",
        help="Ignore not found requests",
    )

    ingest_api_parser.set_defaults(func=ingest_api_cmd)

    # Features command
    process_parser = subparsers.add_parser("features", help="Creates features suite")
    process_parser.set_defaults(func=features_cmd)

    # Train command
    train_parser = subparsers.add_parser("train", help="Train model")
    train_parser.add_argument(
        "--classifier",
        default="lightgbm",
        choices=CLASSIFIER_REGISTRY,
        help="Classifier algorithm",
    )
    train_parser.add_argument(
        "--reducer",
        default="none",
        choices=REDUCER_REGISTRY,
        help="Dimensionality reducer choice",
    )
    train_parser.add_argument(
        "--scaler",
        default="robust",
        choices=SCALER_REGISTRY,
        help="Numerical scaler choice",
    )
    train_parser.add_argument(
        "--encoder",
        default="onehot",
        choices=ENCODER_REGISTRY,
        help="Categorical data encoder choice",
    )
    train_parser.add_argument(
        "--imputer",
        default="median",
        choices=IMPUTER_REGISTRY,
        help="Missing data imputer choice",
    )
    train_parser.add_argument(
        "--n_trials",
        "-n",
        default=10,
        type=min_trials_folds,
        help="Number of hyperparameters combinations to test",
    )
    train_parser.add_argument(
        "--cv_folds",
        "-cv",
        default=5,
        type=min_cv_folds,
        help="Number of cross validation folds",
    )
    train_parser.add_argument(
        "--seed",
        "-r",
        default=42,
        type=int,
        nargs="?",
        const=random_seed(),
        help="Randomize training seed",
    )

    train_parser.add_argument(
        "--test", "-t", default=False, action="store_true", help="Run a quick test"
    )
    train_parser.add_argument(
        "--gpu", default=False, action="store_true", help="Use gpu with lightgbm"
    )
    train_parser.set_defaults(func=train_cmd)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    log_path = Path.cwd() / "log.log"
    logger = init_logger(log_path, logging.INFO)

    # Create dependencies
    try:
        deps = Dependencies(
            logger=logger,
            project_root=Path(__file__).parents[2],
            package_root=Path(__file__).parents[0],
            log_path=log_path,
            version=get_git_hash(short=True),
            git_branch=get_git_branch(),
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


def min_cv_folds(x):
    x = int(x)
    if x < 2:
        raise argparse.ArgumentTypeError("Minimum cv folds is 2")
    return x


def min_trials_folds(x):
    x = int(x)
    if x < 2:
        raise argparse.ArgumentTypeError("Minimum trials is 2")
    return x


def random_seed() -> int:
    return random.randint(1, 1000)
