#!/usr/bin/env python3
import concurrent.futures
import sys
import boto3
import botocore
import click

s3 = boto3.client('s3')

def restore_s3_object(bucket_name, key, tier):
    escaped_key = key.rstrip()
    try:
        if tier == 'Fastest':
            # Get the storage class of the object
            response = s3.head_object(Bucket=bucket_name, Key=key)
            storage_class = response['StorageClass']

            if storage_class == 'STANDARD_IA':
                tier = 'Expedited'
            elif storage_class == 'GLACIER':
                tier = 'Standard'
            elif storage_class == 'DEEP_ARCHIVE':
                tier = 'Bulk'
            else:
                print('Unknown storage class type')


        response = s3.restore_object(
            Bucket=bucket_name,
            Key=escaped_key,
            RestoreRequest={
                'Days': 30,
                'GlacierJobParameters': {
                    'Tier': tier
                }
            }
        )
        return response
    except botocore.exceptions.ClientError as e:
        return {'error': e, "key": key}


@click.command()
@click.argument('bucket_name')
@click.option('--jobs', default=30, help="parallel jobs")
@click.option('--prefix', default="", help="optional prefix to prepend on each key bucket/PREFIX/key, no slashes on start")
@click.option('--tier', default="Bulk", help="You can use Expedited, Standard, Bulk or fastest")
def export(bucket_name, jobs, prefix, tier):
    """

    cat keys | parallel.py bucket_name

    Reads from stdin the keys and sends export request

    --tier is one of:
        Expedited – Expedited retrievals allow you to quickly access your data that's stored in the S3 Glacier Flexible Retrieval storage class or the S3 Intelligent-Tiering Archive Access tier when occasional urgent requests for restoring archives are required. For all but the largest archives (more than 250 MB), data accessed by using Expedited retrievals is typically made available within 1–5 minutes. Provisioned capacity ensures that retrieval capacity for Expedited retrievals is available when you need it. For more information, see Provisioned Capacity.
        Standard – Standard retrievals allow you to access any of your archives within several hours. Standard retrievals are typically completed within 3–5 hours. This is the default option for retrieval requests that do not specify the retrieval option.
        Bulk – Bulk retrievals are the lowest-cost S3 Glacier retrieval option, which you can use to retrieve large amounts, even petabytes, of data inexpensively in a day. Bulk retrievals are typically completed within 5–12 hours.

        read more:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/restore_object.html
            https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html#api-downloading-an-archive-two-steps-retrieval-options

        Note: When using something different from Bulk you can get an error that the service is not available.
        Provisioned capacity helps ensure that your retrieval capacity for Expedited retrievals is available when you need it. Each unit of capacity provides that at least three Expedited retrievals can be performed every 5 minutes and provides up to 150 megabytes per second (MBps) of retrieval throughput.
        If your workload requires highly reliable and predictable access to a subset of your data in minutes, we recommend that you purchase provisioned retrieval capacity. Without provisioned capacity, Expedited retrievals are typically accepted, except for rare situations of unusually high demand. However, if you require access to Expedited retrievals under all circumstances, you must purchase provisioned retrieval capacity.

        Expideted - do not work with Glacier Flexible Retrieval (formerly Glacier)

        Read more on Provisioned capacity
            https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html#api-downloading-an-archive-two-steps-retrieval-options



    """
    tier = tier.capitalize()

    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = []
        for line in sys.stdin:
            # date, time, size, key = line.strip().split()
            key = line
            full_key = prefix + key
            futures.append(executor.submit(restore_s3_object, bucket_name, full_key, tier))

        # Wait for all the futures to complete
        numner_of_tasks = len(futures)
        print(numner_of_tasks)
        count = 0
        for future in concurrent.futures.as_completed(futures):
            response = future.result()
            count += 1
            if count % 100 == 0:
                print(count)

            if 'error' in response:
                print("Error:", response['key'], response['error'])
                continue

            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                error = response['ExportTasks'][0]['Error']
                error_message = error['Code'] + ': ' + error['Message']
                print('Error occurred during S3 export: ' + error_message)



if __name__ == '__main__':
    export()
