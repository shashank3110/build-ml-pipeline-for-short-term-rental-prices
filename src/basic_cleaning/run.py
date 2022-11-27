#!/usr/bin/env python
"""
data cleaning step with wandb and mlflow.
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    # project_name and experiment_name set as wandb env variables in main.py
    # run = wandb.init(job_type="basic_cleaning")

    with wandb.init(job_type="basic_cleaning") as run:
        run.config.update(args)

        # Download input artifact. This will also log that this script is using this
        # particular version of the artifact
        artifact_local_path = run.use_artifact(args.input_artifact).file()
        logging.info("Input File download successful")

        ######################
        # YOUR CODE HERE     #
        ######################
        df = pd.read_csv(artifact_local_path)
        # dropping rows outside given price range
        logging.info("Filtering price range")
        valid_price_indices = df['price'].between(args.min_price,args.max_price)
        df = df[valid_price_indices].copy()

        # Convert last_review to datetime
        logging.info("cast last_review date to datetime type")
        df['last_review'] = pd.to_datetime(df['last_review'])

        # limit the boundaries 
        logging.info("Filtering latitude/longitude range")
        valid_boundary_indices = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
        df = df[valid_boundary_indices].copy()

        df.to_csv('clean_sample.csv',index=False)

        artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
        )  

        logging.info("Uploading cleaned data to WandB")

        artifact.add_file("clean_sample.csv")
        run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="cleaning job for data prep.")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='name of the input data file with version',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='file name for cleaned output data',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='output artifact type',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='output artifact description',
        required=False,
        default='input data after basic cleaning'
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='min. price filter',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='max. price filter',
        required=True
    )


    args = parser.parse_args()

    go(args)
