#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    
    # Download artifact
    logger.info(f'Download artifact {args.input_artifact} from W&B')
    artifact_local_path = wandb.use_artifact(args.input_artifact).file()
    
    # Read artifact
    logger.info(f'Read artifact to Pandas dataframe')
    df = pd.read_csv(artifact_local_path)
    
    # Drop outliers
    logger.info('Dropping outliers')
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    
    # Convert last_review to datetime
    logger.info('Convert last_review to datetime')
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    # Drop rows in the dataset that are not in the proper geolocation
    logger.info('Dropping rows in the dataset that are not in the proper geolocation')
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    # Save the results to a CSV file
    logger.info('Saving results to CSV file')
    df.to_csv(args.output_artifact, index=False)
    
    # Upload it to W&B
    logger.info('Uploading artifact to W&B')
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact filename",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact filename",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact content",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum PRICE (float) value for column filtering",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum PRICE (float) value for column filtering",
        required=True
    )

    args = parser.parse_args()

    go(args)
