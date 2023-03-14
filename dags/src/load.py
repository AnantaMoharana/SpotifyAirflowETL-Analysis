import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def load_tables(key,data,bucketname):
    #Get S3 hook
    hook=S3Hook('s3_connection')

    #Convert the json file into a string
    json_string_data=json.dumps(data)

    #laod the data into an S3 bucket
    hook.load_string(
        string_data=json_string_data,
        key=key,
        bucket_name=bucketname,
        replace=True

    )





