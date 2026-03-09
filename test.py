import mlflow

client = mlflow.MlflowClient()

versions = client.get_latest_versions("inat_scorer_logistic")
for v in versions:
    print(v.run_id)
    print(v.source)  # ← this is the full artifact URI
