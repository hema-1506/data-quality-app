import json
import re
import boto3
import urllib.request

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("DataQualityResults")

EMAIL_DETECT = r'[\w\.-]+@[\w\.-]+'
EMAIL_VALIDATE = r'^[\w\.-]+@[\w\.-]+\.\w{2,}$'
TIME_PATTERN = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC'

API_KEY = "06bdf17ee8ae4aa6ad4722961fa53338"


def check_email_api(email):
    try:
        url = f"https://api.zerobounce.net/v2/validate?api_key={API_KEY}&email={email}"
        print("API CALL:", url)

        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            print("API RESPONSE:", data)
            return data.get("status") == "valid"

    except Exception as e:
        print("EMAIL API ERROR:", str(e))
        return False


def convert_time_api(timestamp):
    try:
        url = "https://mn41olngaj.execute-api.us-east-1.amazonaws.com/convert-time"

        parts = timestamp.split(" ")
        if len(parts) < 2:
            return None

        date_part = parts[0]
        time_part = parts[1]

        payload = json.dumps({
            "date": date_part,
            "time": time_part,
            "sourceTimeZone": "UTC",
            "targetTimeZones": ["Asia/Kolkata"]
        }).encode("utf-8")

        req = urllib.request.Request(url, data=payload)
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            print("TIME API RESPONSE:", data)
            return data

    except Exception as e:
        print("TIME API ERROR:", str(e))
        return None


def normalize_text(value):
    if value is None:
        return ""
    return str(value).replace("\ufeff", "").replace("\r", "").replace("\n", "").strip()


def process_structured_data(job_id, rows):
    total_rows = len(rows)
    available_fields = set()

    for row in rows:
        if isinstance(row, dict):
            available_fields.update(k.lower() for k in row.keys())

    result = {
        "job_id": job_id,
        "status": "completed",
        "rows": total_rows,
        "available_fields": list(available_fields)
    }

    if "name" in available_fields:
        missing_names = 0
        for row in rows:
            name = normalize_text(row.get("name", ""))
            if not name:
                missing_names += 1
        result["missing_names"] = missing_names

    if "email" in available_fields:
        valid = 0
        invalid = 0
        duplicate_emails = 0
        email_seen = set()

        for row in rows:
            email = normalize_text(row.get("email", "")).lower().rstrip(".,;:!|")

            if not email:
                continue

            if email in email_seen:
                duplicate_emails += 1
            else:
                email_seen.add(email)

            if not re.fullmatch(EMAIL_VALIDATE, email):
                invalid += 1
                continue

            is_valid = check_email_api(email)
            if is_valid:
                valid += 1
            else:
                invalid += 1

        result["valid_emails"] = valid
        result["invalid_emails"] = invalid
        result["duplicate_emails"] = duplicate_emails

    if "age" in available_fields:
        missing_age_count = 0
        invalid_age_count = 0

        for row in rows:
            age = row.get("age", "")

            if age is None or str(age).strip() == "":
                missing_age_count += 1
                continue

            try:
                age_value = int(age)
                if age_value < 0 or age_value > 120:
                    invalid_age_count += 1
            except Exception:
                invalid_age_count += 1

        result["missing_age_count"] = missing_age_count
        result["invalid_age_count"] = invalid_age_count

    if "gender" in available_fields:
        allowed_genders = {"male", "female", "other"}
        missing_gender_count = 0
        invalid_gender_count = 0

        for row in rows:
            gender = normalize_text(row.get("gender", "")).lower()

            if not gender:
                missing_gender_count += 1
            elif gender not in allowed_genders:
                invalid_gender_count += 1

        result["missing_gender_count"] = missing_gender_count
        result["invalid_gender_count"] = invalid_gender_count

    if "timestamp" in available_fields:
        time_errors = 0
        converted_times = []

        for row in rows:
            timestamp = normalize_text(row.get("timestamp", ""))

            if not timestamp:
                continue

            if not re.fullmatch(TIME_PATTERN, timestamp):
                time_errors += 1
                continue

            time_result = convert_time_api(timestamp)

            if time_result:
                try:
                    converted_times.append({
                        "original": timestamp,
                        "converted": time_result["results"][0]["localDateTime"]
                    })
                except Exception:
                    time_errors += 1
            else:
                time_errors += 1

        result["time_errors"] = time_errors
        result["converted_times"] = converted_times

    return result


def process_text_data(job_id, data):
    if isinstance(data, list):
        data = "\n".join([str(x) for x in data])

    lines = str(data).split("\n")
    total_rows = len(lines)

    missing = 0
    valid = 0
    invalid = 0
    time_errors = 0
    converted_times = []

    for line in lines:
        try:
            print("RAW:", repr(line))
            line = normalize_text(line)
            print("CLEAN:", repr(line))

            if not line:
                missing += 1
                continue

            if not any(char.isdigit() for char in line) and "@" not in line:
                continue

            time_match = re.search(TIME_PATTERN, line)
            if time_match:
                timestamp = time_match.group(0)
                print("TIME DETECTED:", timestamp)

                time_result = convert_time_api(timestamp)

                if time_result:
                    try:
                        converted_times.append({
                            "original": timestamp,
                            "converted": time_result["results"][0]["localDateTime"]
                        })
                    except Exception:
                        time_errors += 1
                else:
                    time_errors += 1

            match = re.search(EMAIL_DETECT, line)
            if not match:
                continue

            email = match.group(0)
            print("PROCESSING EMAIL:", email)

            email = email.replace("\r", "").replace("\n", "").strip().lower().rstrip(".,;:!|")
            print("FINAL EMAIL:", repr(email))

            if not re.fullmatch(EMAIL_VALIDATE, email):
                invalid += 1
                continue

            is_valid = check_email_api(email)
            if is_valid:
                valid += 1
            else:
                invalid += 1

        except Exception as e:
            print("LINE ERROR:", str(e))
            continue

    result = {
        "job_id": job_id,
        "status": "completed",
        "rows": total_rows,
        "missing": missing,
        "valid": valid,
        "invalid": invalid,
        "time_errors": time_errors,
        "converted_times": converted_times,
        "available_fields": ["text", "email", "timestamp"]
    }

    return result


def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            body = json.loads(record["body"])

            job_id = body.get("job_id")
            data = body.get("data", "")

            print("===== NEW JOB START =====")
            print("JOB ID:", job_id)

            if not job_id:
                raise ValueError("job_id is missing")

            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                final_result = process_structured_data(job_id, data)
            else:
                final_result = process_text_data(job_id, data)

            table.put_item(Item=final_result)

            print("SAVED SUCCESSFULLY:", job_id)
            print({
                "event": "SUMMARY",
                "job_id": job_id,
                "result": final_result
            })

        except Exception as e:
            print("ERROR:", str(e))
            raise e