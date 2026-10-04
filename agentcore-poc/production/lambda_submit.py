"""POST /briefs - accept a brief request and return immediately.

Why this exists: one brief takes 30-60 seconds, and API Gateway gives up at ~29.
So the API does not call the agent at all. It validates, records a job, queues it
and answers 202 with a job id. The worker Lambda does the slow part.

Security note that matters: actor_id comes from the verified JWT, never from the
request body. If the client could choose it, any user could read another user's
long-term memory by claiming their id.
"""
import json
import os
import time
import uuid

import boto3

TABLE = boto3.resource("dynamodb").Table(os.environ["JOBS_TABLE"])
SQS = boto3.client("sqs")
QUEUE_URL = os.environ["QUEUE_URL"]
TTL_DAYS = int(os.getenv("JOB_TTL_DAYS", "30"))


def _claims(event: dict) -> dict:
    """Claims the API Gateway JWT authorizer already verified."""
    return event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})


def _response(code: int, body: dict) -> dict:
    return {"statusCode": code, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}


def handler(event, _context):
    claims = _claims(event)
    user_id = claims.get("sub")
    if not user_id:
        return _response(401, {"error": "unauthenticated"})

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "body must be JSON"})

    prompt = (body.get("prompt") or "").strip()
    if not 10 <= len(prompt) <= 4000:
        return _response(400, {"error": "prompt must be between 10 and 4000 characters"})

    job_id = str(uuid.uuid4())
    now = int(time.time())
    # One conversation = one session. A follow-up turn reuses session_id, which keeps
    # the caller on the same microVM and in the same short-term memory.
    session_id = body.get("session_id") or f"brief-{job_id}"

    TABLE.put_item(Item={
        "job_id": job_id,
        "user_id": user_id,
        "session_id": session_id,
        "status": "QUEUED",
        "prompt": prompt,
        "created_at": now,
        "expires_at": now + TTL_DAYS * 86400,   # DynamoDB TTL - jobs clean themselves up
    })

    message = {"job_id": job_id, "user_id": user_id, "session_id": session_id, "prompt": prompt}
    send = {"QueueUrl": QUEUE_URL, "MessageBody": json.dumps(message)}
    if QUEUE_URL.endswith(".fifo"):
        # FIFO: dedupe on job id, and order per user so one user's follow-up turns
        # never overtake each other.
        send["MessageDeduplicationId"] = job_id
        send["MessageGroupId"] = user_id
    SQS.send_message(**send)

    return _response(202, {"job_id": job_id, "session_id": session_id, "status": "QUEUED",
                           "poll": f"/briefs/{job_id}"})


def get_handler(event, _context):
    """GET /briefs/{job_id} - poll for the result."""
    claims = _claims(event)
    user_id = claims.get("sub")
    job_id = event.get("pathParameters", {}).get("job_id")
    if not user_id:
        return _response(401, {"error": "unauthenticated"})

    item = TABLE.get_item(Key={"job_id": job_id}).get("Item")
    # Check ownership, and return 404 rather than 403 so job ids can't be probed.
    if not item or item.get("user_id") != user_id:
        return _response(404, {"error": "not found"})

    out = {k: item[k] for k in ("job_id", "status", "session_id", "created_at") if k in item}
    for k in ("result", "review_score", "revisions", "latency_ms", "error"):
        if k in item:
            out[k] = item[k]
    return _response(200, json.loads(json.dumps(out, default=str)))
