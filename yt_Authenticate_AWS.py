import boto3
from yt_stats_contexts import *
from yt_stats_pwds import *

aws_session = boto3.Session(aws_access_key_id=root_user_access_key,
                            aws_secret_access_key=root_user_sceret_key,
                            region_name=awsregion)
#aws_session = boto3.Session()

def aws_resource():
    global s3_session
    #s3_session = boto3.resource('s3')
    s3_session = boto3.resource('s3',
                                aws_access_key_id = root_user_access_key,
                                aws_secret_access_key = root_user_sceret_key)
    return s3_session

def aws_client():
    global s3_client
    #s3_client = boto3.client('s3')
    s3_client = boto3.client('s3',
                            aws_access_key_id = root_user_access_key,
                            aws_secret_access_key = root_user_sceret_key
                            )
    return s3_client

def aws_glue_client():
    glue_client = boto3.client('glue',
                            aws_access_key_id = root_user_access_key,
                            aws_secret_access_key = root_user_sceret_key,
                               region_name=awsregion
                            )
    return glue_client
