"""Entry point for every job task: --task picks which step runs."""
import argparse
import sys

# Serverless doesn't install the package, so add the bundle's src folder before importing it.
boot = argparse.ArgumentParser(add_help=False)
boot.add_argument("--src-path", required=True)
sys.path.insert(0, boot.parse_known_args()[0].src_path)

from pyspark.sql import SparkSession

from pipeline import tasks

args = tasks.make_parser().parse_args()
tasks.run(SparkSession.builder.getOrCreate(), args)
