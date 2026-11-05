import argparse
import sys

# Serverless has no installed package: add the bundle's src folder, then import.
boot = argparse.ArgumentParser(add_help=False)
boot.add_argument("--src-path", required=True)
sys.path.insert(0, boot.parse_known_args()[0].src_path)

from pyspark.sql import SparkSession

from partd import pipeline

args = pipeline.make_parser().parse_args()
pipeline.run_check_silver(SparkSession.builder.getOrCreate(), args)
