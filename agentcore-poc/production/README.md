# Production front door

The POC scripts invoke the agent directly with your own credentials. That's fine for a demo and
wrong for production: real callers shouldn't hold AWS credentials, a brief takes longer than an
HTTP request should, and nobody is watching the terminal when something fails.

This stack puts a proper front door on the same runtime.

```
client ──POST /briefs (JWT)──▶ HTTP API ──▶ submit Lambda ──▶ SQS ──▶ worker Lambda ──▶ AgentCore
   └────GET /briefs/{id}────▶ HTTP API ──▶ status Lambda ◀── DynamoDB ◀───────┘   └─▶ S3 + SNS
```

**Why it's split this way.** API Gateway times out around 29 seconds; a brief takes 30–60. So the
API never calls the agent — it records a job and returns `202` with a job id in milliseconds. The
worker Lambda (15-minute ceiling) does the slow call. The queue also gives you retries, a
dead-letter queue, and a concurrency ceiling that doubles as a cost cap.

## Deploy

```bash
cd production
sam build
sam deploy --guided \
  --parameter-overrides \
    AgentRuntimeArn=$(jq -r .agent_runtime_arn ../deploy/state.json) \
    UserPoolId=<your-pool-id> UserPoolClientId=<your-client-id> \
    NotifyEmail=you@example.com
```

No Cognito pool yet? The quickest one that works:

```bash
POOL=$(aws cognito-idp create-user-pool --pool-name brief-users --query UserPool.Id --output text)
CLIENT=$(aws cognito-idp create-user-pool-client --user-pool-id $POOL --client-name brief-cli \
  --explicit-auth-flows ALLOW_USER_PASSWORD_AUTH ALLOW_REFRESH_TOKEN_AUTH \
  --query UserPoolClient.ClientId --output text)
aws cognito-idp admin-create-user --user-pool-id $POOL --username alice --message-action SUPPRESS
aws cognito-idp admin-set-user-password --user-pool-id $POOL --username alice \
  --password 'Str0ng-Passw0rd!' --permanent
```

## Call it

```bash
API=<ApiUrl output from sam deploy>
# Use the ID token: its `aud` claim is the client id, which is what the HTTP API
# JWT authorizer matches against. A Cognito *access* token carries `client_id`
# instead and is rejected by the audience check.
TOKEN=$(aws cognito-idp initiate-auth --auth-flow USER_PASSWORD_AUTH --client-id $CLIENT \
  --auth-parameters USERNAME=alice,PASSWORD='Str0ng-Passw0rd!' \
  --query AuthenticationResult.IdToken --output text)

# submit — returns immediately
curl -s -X POST $API/briefs -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"prompt":"Brief me on AgentCore Memory for a customer call tomorrow."}'
# {"job_id":"7c2f…","session_id":"brief-7c2f…","status":"QUEUED","poll":"/briefs/7c2f…"}

# poll — QUEUED → RUNNING → SUCCEEDED
curl -s $API/briefs/7c2f… -H "Authorization: Bearer $TOKEN" | jq
```

A follow-up turn in the same conversation passes the `session_id` back, which keeps the caller on
the same microVM and inside the same short-term memory:

```bash
curl -s -X POST $API/briefs -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"prompt":"Shorten that to 5 bullets.","session_id":"brief-7c2f…"}'
```

## Two things that are security decisions, not style

**`actor_id` comes from the verified token, never from the request body.** Long-term memory is
keyed on it, so a client that can choose its own actor id can read another user's remembered
preferences and facts. The submit Lambda reads `sub` from the JWT claims the authorizer verified.

**Job ids are checked for ownership, and a mismatch returns 404, not 403.** A 403 confirms the job
exists, which lets someone enumerate ids.

## What's wired up beyond the happy path

| Concern | Where it's handled |
|---|---|
| Slow agent call | Worker Lambda, 900 s timeout; botocore `read_timeout=900`, retries off |
| Duplicate work | botocore retries disabled; SQS FIFO dedupe if you switch to a `.fifo` queue |
| Failures | 3 attempts then the DLQ; job row marked `FAILED` with the error; alarm on DLQ depth |
| Runaway cost | `ReservedConcurrentExecutions: 10` on the worker caps parallel agent sessions; API throttling caps request rate |
| Quality regression | CloudWatch alarm when average `ReviewScore` drops below 7 for 30 minutes |
| Large outputs | Markdown goes to S3 (90-day lifecycle); DynamoDB holds metadata only |
| Data retention | Job rows expire via DynamoDB TTL after 30 days; AgentCore Memory events after 30 |
| Blue-green | `AgentEndpoint` parameter — point prod at a named endpoint, not `DEFAULT` |

## Alternative: skip the API layer entirely

AgentCore Runtime can verify caller JWTs itself (`authorizerConfiguration.customJWTAuthorizer`
with your OIDC discovery URL). Clients then call `InvokeAgentRuntime` directly with their identity
token, and you delete the API, the queue and both Lambdas.

Fewer moving parts, and you lose: request throttling per client, the 202-plus-poll pattern
(callers must hold a long connection), result storage, notifications, and a place to put anything
that isn't the agent. Good for internal service-to-service calls; thin for a user-facing product.
