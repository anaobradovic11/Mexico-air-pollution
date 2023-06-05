import logging
import os
import sys
from pathlib import Path

import click
import pandas as pd

from app.mongo import MongoDBClient

logger = logging.getLogger(__name__)


@click.command()
@click.option("-d", "--dataset-path", required=True, type=click.Path())
@click.option("-t", "--table-name", required=True, type=str)
def import_data(dataset_path: Path, table_name: str) -> None:
    if not os.path.exists(dataset_path):
        logging.info("Dataset on path: '{dataset_path}' doesn't exist.")
        sys.exit(1)

    client = MongoDBClient()
    client.init_database()

    dataset: pd.DataFrame = pd.read_csv(dataset_path)
    data_points = dataset.to_dict(orient="records")

    client.insert(data_points, table_name)

    logger.info("Import completed. Shutting down...")


if __name__ == "__main__":
    import_data()
