from sklearn.pipeline import Pipeline


def describe_pipeline(pipeline: Pipeline) -> dict:
    """
    Returns a human-readable description of all pipeline steps and their params.
    Useful for logging to MLflow as a run artifact (saved as pipeline_description.json).
    """
    description = {}
    for name, step in pipeline.steps:
        if hasattr(step, "steps"):  # nested Pipeline (num/cat sub-pipelines)
            description[name] = {
                sub_name: {
                    "class": type(sub_step).__name__,
                    "params": sub_step.get_params(),
                }
                for sub_name, sub_step in step.steps
            }
        elif hasattr(step, "transformers"):  # ColumnTransformer
            description[name] = {
                t_name: {
                    "class": (
                        type(t_step).__name__
                        if not hasattr(t_step, "steps")
                        else "Pipeline"
                    ),
                    "columns": cols,
                }
                for t_name, t_step, cols in step.transformers
            }
        else:
            description[name] = {
                "class": type(step).__name__,
                "params": step.get_params(),
            }
    return description
