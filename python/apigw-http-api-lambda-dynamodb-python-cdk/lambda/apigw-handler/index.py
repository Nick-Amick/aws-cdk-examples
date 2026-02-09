# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    # Log request context for security investigations
    request_id = context.request_id
    source_ip = event.get("requestContext", {}).get("identity", {}).get("sourceIp", "unknown")
    
    table = os.environ.get("TABLE_NAME")
    
    # Structured logging with security context
    logger.info(json.dumps({
        "message": "Processing request",
        "requestId": request_id,
        "sourceIp": source_ip,
        "tableName": table,
    }))
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(json.dumps({
                "message": "Received payload",
                "requestId": request_id,
                "itemId": item.get("id"),
            }))
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            message = "Successfully inserted data!"
            logger.info(json.dumps({
                "message": "Data inserted successfully",
                "requestId": request_id,
                "itemId": id,
            }))
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info(json.dumps({
                "message": "Received request without payload, using default data",
                "requestId": request_id,
            }))
            default_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": default_id},
                },
            )
            message = "Successfully inserted data!"
            logger.info(json.dumps({
                "message": "Default data inserted successfully",
                "requestId": request_id,
                "itemId": default_id,
            }))
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "message": "Error processing request",
            "requestId": request_id,
            "error": str(e),
            "errorType": type(e).__name__,
        }))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Internal server error"}),
        }
