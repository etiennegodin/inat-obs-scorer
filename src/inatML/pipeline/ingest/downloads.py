import logging
from pathlib import Path

import duckdb
import pandas as pd
from duckdb import CatalogException
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


def ingest_downloads(
    con: duckdb.DuckDBPyConnection, downloads_path: Path
) -> list[Path]:
    create_query = f"""CREATE TABLE downloads AS 
            SELECT *
            FROM read_csv_auto('{downloads_path}/*.csv')"""
    files = [file for file in downloads_path.rglob("*.csv")]
    try:
        con.execute(create_query)
        return files
    except CatalogException:
        con.execute("DROP TABLE IF EXISTS downloads")
        try:
            con.execute(create_query)
            return files
        except Exception as e:
            raise e


def select_sample_observations(
    con: duckdb.DuckDBPyConnection,
    target_count: int = 40000,
    filters: dict = {"user_obs": 20, "old": "2020-01-01", "new": "2024-01-01"},
) -> None:
    df = con.execute("SELECT * FROM downloads").df()
    logger.debug(f"Raw import {df.shape[0]}")

    # Drop columns NaNs
    df = df.drop(columns=["time_observed_at", "license", "time_zone"])

    # Drop rows with NaNs
    df = df.dropna(subset=["observed_on"])

    # Remove duplicates
    df = df.drop_duplicates()
    logger.debug(f"After cleanup {df.shape[0]}")

    # Remove old observations
    before = "2010-01-01"
    df = df[df["observed_on"] >= before]
    logger.debug(f"After old obs {df.shape[0]}")

    # Observation count filter
    df_user = df.groupby(by="user_id").count().reset_index("user_id")
    # Keep only two cols and rename
    df_user = df_user[["user_id", "id"]].rename(columns={"id": "obs_count"})
    # Keep only with obs over thresold
    df_user = df_user[df_user["obs_count"] >= filters["user_obs"]]
    logger.debug(f"After obs count filter {df_user.shape[0]} users")

    # Temporal filter
    df_temporal = pd.merge(df, df_user, on=["user_id"], how="inner")
    min = (
        df_temporal[["user_id", "observed_on"]]
        .groupby(by="user_id")
        .min()
        .reset_index("user_id")
        .rename(columns={"observed_on": "oldest"})
    )
    max = (
        df_temporal[["user_id", "observed_on"]]
        .groupby(by="user_id")
        .max()
        .reset_index("user_id")
        .rename(columns={"observed_on": "newest"})
    )
    df_user = pd.merge(df_user, min, on=["user_id"], how="inner")
    df_user = pd.merge(df_user, max, on=["user_id"], how="inner")
    df_user = df_user[df_user["oldest"] <= filters["old"]]
    df_user = df_user[df_user["newest"] >= filters["new"]]
    logger.debug(f"After obs count filter {df_user.shape[0]} users")

    # First half with low count user
    low_users = []
    count = 0
    df_user_low = df_user.sort_values("obs_count")

    for _, row in df_user_low.iterrows():
        if count > target_count / 2:
            break

        low_users.append(row["user_id"])
        count += row["obs_count"]

    df_low = df[df["user_id"].isin(low_users)]
    logger.debug(f"{df_low.shape[0]} observations from {len(low_users)} low user")

    # Second half with high count user sampled
    df_user_high = df_user[~df_user["user_id"].isin(low_users)]
    df_user_high_obs = pd.merge(df, df_user_high, on=["user_id"], how="inner")
    total = df_user_high_obs["id"].nunique()
    split_percentage = (target_count / 2) / total
    _, df_high_sample = train_test_split(
        df_user_high_obs,
        test_size=split_percentage,
        shuffle=True,
        stratify=df_user_high_obs["user_id"],
        random_state=43,
    )

    logger.debug(
        f"{df_high_sample.shape[0]} observations from"
        f"{df_high_sample['user_id'].nunique()} high user"
    )

    df_out = pd.concat([df_low, df_high_sample])
    logger.info(f"Selected {df_out.shape[0]} observations")
    create_query = """CREATE TABLE obs_sample AS
                    SELECT * 
                    FROM df_out
                    ORDER BY uuid ASC"""
    try:
        con.execute(create_query)
    except CatalogException:
        con.execute("DROP TABLE obs_sample")
        try:
            con.execute(create_query)
        except Exception as e:
            raise e
