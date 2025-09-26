import boto3
import requests
import os
import time
import json
from datetime import datetime, timezone
from openai import OpenAI


logs_client = boto3.client("logs")
ses_client = boto3.client("ses")
sns_client = boto3.client("sns")
SNS_REPORT_TOPIC_ARN = os.environ["SNS_REPORT_TOPIC_ARN"]

token = os.environ["GITHUB_TOKEN"]
endpoint = "https://models.github.ai/inference"
model = "openai/gpt-5-mini"


SPARK_API_TOKEN = os.environ["SPARK_API_TOKEN"]
SPARK_UAT_URL = os.environ["SPARK_UAT_URL"]


LOG_GROUP = os.environ.get("LOG_GROUP")


client = OpenAI(
    base_url=endpoint,
    api_key=token,
)


def get_alarm_time(sns_message):
    try:
        msg_json = json.loads(sns_message)
        time_str = msg_json.get("StateChangeTime")
        alarm_time = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        return alarm_time
    except Exception as e:
        print(f"Failed to parse alarm time: {e}")
        return datetime.now(timezone.utc)


def query_logs(alarm_time):
    start_time = int(alarm_time.timestamp()) - 300  # 5 min before
    end_time = int(alarm_time.timestamp()) + 300  # 5 min after
    query_string = "fields @timestamp, @message | sort @timestamp desc | limit 100"

    start_query_response = logs_client.start_query(
        logGroupName=LOG_GROUP,
        startTime=start_time,
        endTime=end_time,
        queryString=query_string,
    )
    query_id = start_query_response["queryId"]

    while True:
        response = logs_client.get_query_results(queryId=query_id)
        if response["status"] == "Complete":
            break
        time.sleep(1)

    logs = []
    for result in response["results"]:
        for field in result:
            if field["field"] == "@message":
                logs.append(field["value"])
    return "\n".join(logs) if logs else "No relevant logs found."


def call_openai(prompt):

    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        model=model,
    )


    return response.choices[0].message.content


def call_spark(prompt):
    try:
        payload = {
            "messages": [
                {"role": "system", "content": "Answer from the given context only"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.5,
            "n": 1,
            "stream": False,
            "presence_penalty": 0,
            "frequency_penalty": 0,
            "top_p": 1,
        }

        headers = {
            "app_id": "sparkassist",
            "api-key": SPARK_API_TOKEN,
            "Content-Type": "application/json",
        }
        spark_response = requests.post(
            SPARK_UAT_URL,
            headers,
            json=payload,
            timeout=15,
        )

        data = spark_response.json()
        return data["choices"][0]["message"]["content"]

    except Exception as e:
        print(f"Spark API call failed: {e}")
        return "Failed to generate investigation report."


def send_report_via_sns(subject, message):
    try:
        sns_client.publish(
            TopicArn=SNS_REPORT_TOPIC_ARN, Subject=subject, Message=message
        )
        print("Report published to SNS for email delivery.")
    except Exception as e:
        print(f"Failed to publish SNS email: {e}")


def lambda_handler(event, context):
    try:
        sns_message = event["Records"][0]["Sns"]["Message"]
        # print(f"Received SNS message: {sns_message}")
        alarm_time = get_alarm_time(sns_message)

        logs_text = query_logs(alarm_time)
        # print(f"Fetched logs:\n{logs_text}")

        prompt = f"""
        You are a principal SRE. Analyze the provided alert and diagnostic data. 
        Formulate a root cause hypothesis, assess the business impact, and provide a primary and 
        secondary recommendation. Respond in JSON
        
        Alert: 500 series error detected at {alarm_time.isoformat()}

        Logs:
        {logs_text}

        Please generate a clear investigation summary, possible root causes, and recommended actions.
        """

        #        report = call_spark(prompt)
        report = call_openai(prompt)
        send_report_via_sns(
            subject="AI Investigation Report - portfolio-data-service", message=report
        )

        return {"statusCode": 200, "body": "Report sent successfully"}

    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {"statusCode": 500, "body": str(e)}