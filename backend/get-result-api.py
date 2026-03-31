import json
import boto3
from decimal import Decimal
from urllib.parse import parse_qs

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("DataQualityResults")


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def lambda_handler(event, context):
    print("FULL EVENT:", json.dumps(event))

    job_id = None

    if event.get("queryStringParameters"):
        job_id = event["queryStringParameters"].get("job_id")

    if not job_id and event.get("rawQueryString"):
        parsed = parse_qs(event["rawQueryString"])
        job_id = parsed.get("job_id", [None])[0]

    print("JOB ID RECEIVED:", job_id)

    if not job_id:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error": "Missing job_id"
            })
        }

    response = table.get_item(Key={"job_id": str(job_id)})
    print("DYNAMO RESPONSE:", response)

    item = response.get("Item")

    if not item:
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "job_id": str(job_id),
                "status": "pending",
                "message": "Result not ready yet"
            })
        }

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json"
        },
        "body": json.dumps(item, default=decimal_default)
    }