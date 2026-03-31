import json
import boto3
import uuid
import re
import urllib.request

sqs = boto3.client("sqs", region_name="us-east-1")

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/044290396042/document-analysis-queue"

EMAIL_DETECT = r'[\w\.-]+@[\w\.-]+'
EMAIL_VALIDATE = r'^[\w\.-]+@[\w\.-]+\.\w{2,}$'
API_KEY = "06bdf17ee8ae4aa6ad4722961fa53338"


def check_email_api(email):
    try:
        url = f"https://api.zerobounce.net/v2/validate?api_key={API_KEY}&email={email}"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            return data.get("status") == "valid"
    except Exception:
        return False


def normalize_text(value):
    if value is None:
        return ""
    return str(value).replace("\ufeff", "").replace("\r", "").replace("\n", "").strip()


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method")

    if method == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            "body": ""
        }

    try:
        body = json.loads(event.get("body", "{}"))
        data = body.get("data", "")

        if not data:
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Allow-Methods": "*",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({
                    "error": "No data provided"
                })
            }

        job_id = str(uuid.uuid4())

        # Keep background processing through SQS
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({
                "job_id": job_id,
                "data": data
            })
        )

        # Immediate summary for frontend
        if isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], dict):
                rows = len(data)
                missing = 0
                valid = 0
                invalid = 0

                for row in data:
                    row_missing = False

                    for value in row.values():
                        if normalize_text(value) == "":
                            row_missing = True
                    if row_missing:
                        missing += 1

                    if "email" in row:
                        email = normalize_text(row.get("email", "")).lower().rstrip(".,;:!|")
                        if email:
                            if re.fullmatch(EMAIL_VALIDATE, email) and check_email_api(email):
                                valid += 1
                            else:
                                invalid += 1

                return {
                    "statusCode": 200,
                    "headers": {
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Allow-Methods": "*",
                        "Content-Type": "application/json"
                    },
                    "body": json.dumps({
                        "rows": rows,
                        "missing": missing,
                        "valid": valid,
                        "invalid": invalid
                    })
                }

            else:
                data = "\n".join([str(x) for x in data])

        lines = str(data).split("\n")

        rows = len(lines)
        missing = 0
        valid = 0
        invalid = 0

        for line in lines:
            line = normalize_text(line)

            if not line:
                missing += 1
                continue

            match = re.search(EMAIL_DETECT, line)
            if match:
                email = match.group(0).lower().rstrip(".,;:!|")

                if re.fullmatch(EMAIL_VALIDATE, email) and check_email_api(email):
                    valid += 1
                else:
                    invalid += 1

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "rows": rows,
                "missing": missing,
                "valid": valid,
                "invalid": invalid
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error": str(e)
            })
        }