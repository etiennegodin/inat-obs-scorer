import logging

from ..app.container import Dependencies
from ..pipeline.model import config, core

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    # Data Loader
    train, val, test, split_seed = core.load(deps.DB_PATH)

    # Initialise pipeline configs
    conf = config.PipelineConfig()

    # Store features from dataframe
    conf.set_features(test)

    # Override features type
    conf.change_feature_type("oauth_application_id")

    pipe = core.build_pipeline(conf)

    print(pipe)

    # Classifier

    logger.info("Model workflow")
