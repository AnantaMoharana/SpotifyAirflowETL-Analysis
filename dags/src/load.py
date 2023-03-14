import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook


def load_tables(key,data,bucketname):
    #Get S3 hook
    hook=S3Hook('aws_connection')

    #Convert the json file into a string
    json_string_data=json.dumps(data)

    #laod the data into an S3 bucket
    hook.load_string(
        string_data=json_string_data,
        key=key,
        bucket_name=bucketname,
        replace=True

    )

def json_to_csv(pay_load):
    #Convert the json files for the tables into csv files

    #make aws connection
    hook=LambdaHook('aws_connection',region_name='us-east-1')
    #invoke the funtion
    load=json.dumps(pay_load)
    hook.invoke_lambda(function_name="json_to_csv",payload=load)







