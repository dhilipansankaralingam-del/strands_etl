#!/usr/bin/env bash
# Step 1 - Enable CloudWatch Transaction Search (ONE TIME per account+region).
#
# This is what turns OpenTelemetry spans into searchable span documents in the
# `aws/spans` log group. Without it:
#   * GenAI Observability shows no sessions, traces or spans
#   * AgentCore Evaluations fails with "no span documents"
# and nothing else complains, so it is worth verifying rather than assuming.
set -euo pipefail
: "${AWS_REGION:?source deploy/config.env first}"; : "${AWS_ACCOUNT_ID:?}"

echo "==> Allowing X-Ray to write spans into CloudWatch Logs"
aws logs put-resource-policy --policy-name AgentCoreTransactionSearch --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{
    \"Sid\": \"TransactionSearchXRayAccess\",
    \"Effect\": \"Allow\",
    \"Principal\": {\"Service\": \"xray.amazonaws.com\"},
    \"Action\": [\"logs:PutLogEvents\", \"logs:CreateLogStream\"],
    \"Resource\": [
      \"arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:aws/spans\",
      \"arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:aws/spans:*\",
      \"arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:/aws/application-signals/data\",
      \"arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:/aws/application-signals/data:*\"
    ],
    \"Condition\": {
      \"ArnLike\": {\"aws:SourceArn\": \"arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:*\"},
      \"StringEquals\": {\"aws:SourceAccount\": \"${AWS_ACCOUNT_ID}\"}
    }
  }]
}" >/dev/null
echo "    resource policy in place"

echo "==> Switching trace segment destination to CloudWatch Logs"
# Only call update when it is actually needed: the API rejects setting the
# destination to what it already is. Real failures are NOT swallowed - a silent
# failure here stays invisible until evaluations fail with a confusing message.
CURRENT=$(aws xray get-trace-segment-destination --query Destination --output text)
if [ "$CURRENT" = "CloudWatchLogs" ]; then
  echo "    already set to CloudWatchLogs"
else
  aws xray update-trace-segment-destination --destination CloudWatchLogs
fi

echo "==> Indexing 100% of spans (POC setting; 1% is the free tier)"
aws xray update-indexing-rule --name "Default" \
  --rule '{"Probabilistic": {"DesiredSamplingPercentage": 100}}' >/dev/null

echo "==> Waiting for the destination to report CloudWatchLogs (up to 5 min)"
for i in $(seq 1 30); do
  DEST=$(aws xray get-trace-segment-destination --query Destination --output text)
  STATUS=$(aws xray get-trace-segment-destination --query Status --output text)
  echo "    Destination=$DEST Status=$STATUS"
  if [ "$DEST" = "CloudWatchLogs" ] && [ "$STATUS" = "ACTIVE" ]; then
    echo
    echo "Transaction Search is ON. Spans will land in the aws/spans log group."
    echo "Invoke the agent, then allow 2-5 minutes before looking for traces."
    exit 0
  fi
  sleep 10
done

cat <<'MSG'

Destination is still not CloudWatchLogs.

That means this step did not take effect, and without it GenAI Observability stays
empty and evaluations fail with "no span documents".

Enable it in the console instead, where errors are visible:
  CloudWatch -> Application Signals -> Transaction Search -> Enable Transaction Search
  -> tick "Ingest spans as structured logs" -> set the indexing percentage -> Save

If the console refuses, the usual cause is missing permissions on your identity:
  xray:UpdateTraceSegmentDestination, xray:GetTraceSegmentDestination,
  xray:UpdateIndexingRule, logs:PutResourcePolicy
MSG
exit 1
