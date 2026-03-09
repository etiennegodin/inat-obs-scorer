import logging

from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    mean_absolute_error,
)

from ..app.container import Dependencies
from ..pipeline.model import config, core

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    # Data Loader
    X_train, y_train, X_val, y_val, X_test, y_test, split_seed = core.load(deps.DB_PATH)

    # Initialise pipeline configs
    conf = config.PipelineConfig()

    # Store features from dataframe
    conf.set_features(X_test)

    # Override features type
    conf.change_feature_type("oauth_application_id")

    pipe = core.build_pipeline(conf)

    logger.info(f"Fitting {conf.experiment_name}")

    pipe.fit(X_train, y_train)

    preds = pipe.predict(X_test)
    # probs = pipe.predict_proba(X_test)  # Returns the probability of each class
    accuracy = pipe.score(X_test, y_test)

    print(f"Accuracy: {accuracy}")
    # Confusion Matrix
    cm = confusion_matrix(y_test, preds)
    print(f"Confusion Matrix:\n{cm}")

    # Classification Report
    report = classification_report(y_test, preds)
    print(f"Classification Report:\n{report}")

    mae = mean_absolute_error(y_test, preds)
    print(f"MAE :\n{mae}")

    # pprint(utils.describe_pipeline(pipe))

    # print(pipe)

    # Classifier

    logger.info("Model workflow")
