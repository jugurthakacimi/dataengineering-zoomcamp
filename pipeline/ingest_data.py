#!/usr/bin/env python
# coding: utf-8

from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype: Dict[str, str] = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates: List[str] = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def run():
    pg_user: str = "root"
    pg_password: str = "root"
    pg_host: str = "localhost"
    pg_port: str = "5432"
    pg_db: str = "ny_taxi"

    year: int = 2021
    month: int = 1

    target_table : str = "yellow_taxi_data"

    chunck_size: int = 10000

    prefix: str = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url: str = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )

    df_iter = pd.read_csv(
        url,
        dtype=dtype, #type: ignore
        parse_dates=parse_dates, #type: ignore
        chunksize=chunck_size,
        compression="gzip",
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table, con=engine, if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


if __name__ == "__main__":
    run()
