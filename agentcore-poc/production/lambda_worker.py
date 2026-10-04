"""SQS worker - the only thing that actually calls the agent runtime.

Runs with a long timeout (15 min ceiling), one message at a time, so a slow brief
never blocks an HTTP request. Failures go back to the queue and then to the DLQ
after a few attempts.
"""
import json
import logging
import os
import time

import boto3
from botocore.config import Config

log = logging.getLogger()
log.setLevel(logging.INFO)

RUNTIME_ARN = os.environ["AGENT_RUNTIME_ARN"]
QUALIFIER = os.getenv("AGENT_ENDPOINT", "DEFAULT")
TABLE = boto3.resource("dynamodb").Table(os.environ["JOBS_TABLE"])
RESULT_BUCKET = os.getenv("RESULT_BUCKET")
TOPIC_ARN = os.getenv("NOTIFY_TOPIC_ARN")

# The agent is slow by design: raise the read timeout and switch off botocore's
# retries, because a retried 60-second call is how you get duplicate work.
agentcore = boto3.client("bedrock-agentcore", config=Config(
    read_timeout=900, connect_timeout=10, retries={"max_attempts": 0}))
s3 = boto3.client("s3")
sns = boto3.client("sns")


def _update(job_id: str, **fields):
    """Set attributes on the job row, quoting names so reserved words are safe."""
    expr = ", ".join(f"#{k} = :{k}" for k in fields)
    TABLE.update_item(
        Key={"job_id": job_id},
        UpdateExpression=f"SET {expr}",
        ExpressionAttributeNames={f"#{k}": k for k in fields},
        ExpressionAttributeValues={f":{k}": v for k, v in fields.items()},
    )


def handler(event, _context):
    for record in event.get("Records", []):
        msg = json.loads(record["body"])
        job_id, user_id = msg["job_id"], msg["user_id"]
        log.info("starting job=%s user=%s", job_id, user_id)
        _update(job_id, status="RUNNING", started_at=int(time.time()))

        try:
            resp = agentcore.invoke_agent_runtime(
                agentRuntimeArn=RUNTIME_ARN,
                qualifier=QUALIFIER,
                runtimeSessionId=msg["session_id"].ljust(33, "0")[:256],
                payload=json.dumps({"prompt": msg["prompt"], "actor_id": user_id}).encode(),
                contentType="application/json",
                accept="application/json",
            )
            body = json.loads(resp["response"].read())

            if body.get("status") != "ok":
                raise RuntimeError(body.get("result", "agent returned an error"))

            fields = {
                "status": "SUCCEEDED",
                "review_score": str((body.get("review_scores") or [None])[-1]),
                "revisions": body.get("revisions", 0),
                "latency_ms": body.get("latency_ms", 0),
                "finished_at": int(time.time()),
            }
            # Large markdown belongs in S3, not in a DynamoDB item (400 KB limit).
            if RESULT_BUCKET:
                key = f"briefs/{user_id}/{job_id}.md"
                s3.put_object(Bucket=RESULT_BUCKET, Key=key,
                              Body=body["result"].encode(), ContentType="text/markdown")
                fields["result_s3"] = f"s3://{RESULT_BUCKET}/{key}"
            else:
                fields["result"] = body["result"][:350_000]
            _update(job_id, **fields)

            if TOPIC_ARN:
                sns.publish(TopicArn=TOPIC_ARN, Subject="Brief ready",
                            Message=json.dumps({"job_id": job_id, "user_id": user_id,
                                                "review_score": fields["review_score"]}))
            log.info("done job=%s score=%s revisions=%s", job_id,
                     fields["review_score"], fields["revisions"])

        except Exception as exc:                      # noqa: BLE001 - we want the job marked
            log.exception("job %s failed", job_id)
            _update(job_id, status="FAILED", error=str(exc)[:1000], finished_at=int(time.time()))
            raise      # re-raise so SQS retries, then routes to the DLQ
