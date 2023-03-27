import concurrent.futures
import sys
import boto3
import click
import os


@click.command()
def print_objects():
    """
    Reads from stdin and outputs commands

    usage:

        Instead of this program first try to use

            aws s3 sync --force-glacier-transfer s3://troweprice export/

        If doesn't work, then do:

            cat glacier-keys | python ./copy-comands.py > export.sh

            cat export.sh | parallel --eta --progress --bar --joblog /tmp/joblog -j 50

        To find the difference between two files

            grep -vFf glacier-keys keys > normal-keys.txt


    """
    # Open the file for reading
    for line in sys.stdin:
        key = line.strip()

        if os.path.exists(f'export/{key}'):
            continue

        print(f'aws s3 cp "s3://troweprice/{key}" "export/{key}"')

if __name__ == '__main__':
    print_objects()
