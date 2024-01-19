import concurrent.futures
import sys

import boto3
import click


def list_objects(bucket_name, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                yield obj


@click.command()
@click.argument("bucket_name")
@click.option("--prefix", default="", help="No slash at start, or empty")
@click.option(
    "--storage_class",
    "-c",
    default=[],
    multiple=True,
    help="List only specific class of objects (GLACIER, STANDARD, STANDARD_IA)",
)
def print_objects(bucket_name, prefix, storage_class):
    """
    list bucket_name prefix

    bucket_name is "some.domain.name"
    prefix is could be empty or "some/prefix" - no slash infront or at the end

    S3 Standard: STANDARD
    S3 Intelligent-Tiering: INTELLIGENT_TIERING
    S3 Standard-Infrequent Access: STANDARD_IA
    S3 One Zone-Infrequent Access: ONEZONE_IA
    S3 Glacier: GLACIER
    S3 Glacier Deep Archive: DEEP_ARCHIVE

    """
    show_all = True
    if storage_class:
        show_all = False

    print(f"Working on {bucket_name} prefix: {prefix}")
    for obj in list_objects(bucket_name, prefix):
        if show_all:
            print(obj["Key"])
        else:
            if obj["StorageClass"] in storage_class:
                print(obj["Key"])


if __name__ == "__main__":
    print_objects()
