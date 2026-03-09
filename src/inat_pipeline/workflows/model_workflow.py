import logging

from ..app.container import Dependencies
from ..pipeline.model import config, prep

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    # Data Loader
    train, val, test, split_seed = prep.load(deps.DB_PATH)

    conf = config.ModelPrepConfig()

    conf.set_features(test)
    print(conf.categorical_features)
    print(conf.numeric_features)
    quit()

    preprocessor = prep.build_preprocessor(conf)

    print(preprocessor)

    # Classifier

    logger.info("Model workflow")
