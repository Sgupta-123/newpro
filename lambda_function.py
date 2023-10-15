import json
import boto3
import os
import json
from datetime import datetime
import logging
logger = logging.getLogger()
logger.setLevel (logging.INFO)
s3 = boto3.resource('s3')
def lambda_handler(event, context):
    logger.info(event)
    s3 = boto3.resource('s3')
    source_bucket_name = event["source_bucket"]
    target_bucket_name = event["target_bucket"]
    source_filepath = event["source_filepath"]
    target_filepath = event["target_filepath"]
    dat_file_name = event["file_prefix"] + event["businessDate"] + ".dat"
    ctl_file_name = event["file_prefix"] + event["businessDate"] + ".ctl"
    dat_source_key = source_filepath + dat_file_name
    ctl_source_key= source_filepath + ctl_file_name
    msg1 = f"dat_source_full_filepath: {dat_source_key}, ctl_source_full_filepath: {ctl_source_key}"
    logger.info(msg1)
    print(msg1)
    dat_target_key = target_filepath + dat_file_name + "_" + event["cntxt_key_id"]
    ctl_target_key = target_filepath + ctl_file_name + "_" + event["cntxt_key_id"]
    msg2 = f"dat_target_full_filepath: {dat_target_key}, ctl_target_full_filepath: {ctl_target_key}"
    logger.info(msg2)
    print(msg2)
    dat_copy_source = {"Bucket": source_bucket_name, "Key": dat_source_key}
    ctl_copy_source = {"Bucket": source_bucket_name, "Key": ctl_source_key}
    client = boto3.client('s3')
    result_ctl = client.list_objects(Bucket=source_bucket_name, Prefix=ctl_source_key)
    if 'Contents' in result_ctl:
        s3.meta.client.copy(ctl_copy_source, target_bucket_name, ctl_target_key)
        p="Control file copied"
        return p
    else:
        p3=f"Failed to archive control file : {ctl_copy_source}"
        return p3
    result_dat= client.list_objects(Bucket=source_bucket_name, Prefix=dat_source_key)
    if 'Contents' in result_dat:
        s3.meta.client.copy(dat_copy_source, target_bucket_name, dat_target_key)
        p1="data file copied"
        return p1
    else:
        p2=f"Failed to archive data file : {dat_copy_source}"
        return p2