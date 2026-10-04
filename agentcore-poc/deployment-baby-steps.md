# Deployment Baby Steps

The shortest safe path from an empty AWS account to a running multi-agent system on
Amazon Bedrock AgentCore. Do the steps in order. After each one there is a **Check**:
do not move on until it passes.

Time: about 30–40 minutes the first time. Run everything from the `agentcore-poc/` folder.

For the reasoning behind each step, see `docs/end-to-end-guide.html` and `docs/deploy-runbook.html`.

---

## Part A — Deploy the POC

### Step 0 — Prerequisites (once)

1. Install and check the tools (Mac, Linux, WSL or AWS CloudShell):
   ```bash
   aws --version            # AWS CLI v2
   python3 --version        # 3.10 or newer
   docker buildx version    # only needed for the Docker path (Step 5b)
   ```
2. Log in to AWS (SSO, profile or access keys). In CloudShell you are already logged in.
   ```bash
   aws sts get-caller-identity
   ```
   **Check:** it prints your account id.
3. Enable the model. Console → *Amazon Bedrock* → *Model access* (us-east-1) → enable
   **Anthropic Claude Sonnet**.
   ```bash
   aws bedrock list-inference-profiles --region us-east-1 \
     --query "inferenceProfileSummaries[?contains(inferenceProfileId,'sonnet')].inferenceProfileId"
   ```
   **Check:** `us.anthropic.claude-sonnet-4-6` is listed. If not, put an ID that is listed into
   `MODEL_ID` in `deploy/config.env`.
4. Your AWS user needs broad rights for this POC (IAM, ECR, S3, Bedrock AgentCore, CloudWatch,
   X-Ray). If you can't get them, forward `docs/access-request.html` to your AWS administrator.
5. Install the Python dependencies:
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt pytest
   ```
6. Optional offline test, no AWS needed:
   ```bash
   python -m pytest -q tests/
   ```
   **Check:** all tests pass.

### Step 1 — Load the config (every new terminal)

```bash
source deploy/config.env
```
**Check:** it prints `Account=<id> Region=us-east-1 Agent=research_orchestrator`.

If the terminal was closed or CloudShell timed out, redo this step and re-activate the venv
(`source .venv/bin/activate`). Progress is kept in `deploy/state.json`, so earlier steps are not lost.

### Step 2 — Turn on observability (once per account and region)

```bash
bash deploy/01_enable_observability.sh
```
This enables CloudWatch Transaction Search. Without it there are no traces, no GenAI
Observability dashboard, and evaluations fail with "no span documents". It can take up to
10 minutes to become active. You can carry on meanwhile.

**Check:** the script finishes without an error.

### Step 3 — Create the IAM roles

```bash
python deploy/02_create_iam_roles.py
```
Creates `multiagent-poc-runtime-role` (used by the agent) and `multiagent-poc-eval-role`
(used by online evaluation). Safe to re-run.

**Check:** both role ARNs are printed and saved in `deploy/state.json`.

### Step 4 — Create the memory

```bash
python deploy/03_create_memory.py
```
Takes 2–3 minutes. Creates short-term memory plus three long-term strategies (preferences, facts,
session summaries).

**Check:** it prints `MEMORY_ID=...` and the status becomes `ACTIVE`.

### Step 5 — Package the agent (pick ONE path)

**5a — No Docker (recommended; works in CloudShell)**
```bash
python deploy/04b_package_and_upload.py
```
Builds a linux/arm64 zip (about 39 MB) and uploads it to `s3://bedrock-agentcore-code-<account>-<region>/`.

**5b — Docker image to ECR**
```bash
bash deploy/04_build_and_push.sh
```
Builds a **linux/arm64** image and pushes it to ECR. If you get `exec format error`, run once:
`docker run --privileged --rm tonistiigi/binfmt --install arm64`.

**Check:** the script prints the zip location (5a) or `Saved image_uri=...` (5b).

### Step 6 — Deploy to AgentCore Runtime

```bash
python deploy/05_deploy_runtime.py
```
It deploys whichever artifact you built last. Force one with `--code` or `--container`.
Waits until the runtime is `READY` (1–3 minutes).

**Check:** the status line shows `READY` and `deploy/state.json` now has `agent_runtime_arn`.

If it ends in `CREATE_FAILED`, see Troubleshooting. The usual cause is IAM not having propagated
yet, so wait a minute and re-run this step.

### Step 7 — Trigger it

```bash
python deploy/06_invoke.py "Write a short report on AgentCore Memory for backend engineers"
```
A full run takes 40–90 seconds. This is normal, so don't cancel it.

**Check:** you get a markdown report, a per-agent latency and token breakdown, and a reviewer score.

**Test memory (optional):**
```bash
python deploy/06_invoke.py --actor alice "For all my reports: I'm a CFO, plain language, under 150 words."
# wait 1–2 minutes for long-term memory to be extracted, then use a NEW session:
python deploy/06_invoke.py --actor alice "Write me a report on AgentCore Runtime."
```
**Check:** the second report is short and jargon-free. Use the same `--actor` both times.

### Step 8 — Dashboard

```bash
python deploy/07_create_dashboard.py
```
**Check:** it prints two console links. Open the CloudWatch dashboard `multiagent-poc-dashboard`,
and *CloudWatch → GenAI Observability → Bedrock AgentCore* for traces. If traces are empty, wait
for Step 2 to finish and invoke again.

### Step 9 — Evaluate

```bash
python eval/run_eval.py              # on-demand regression run, 10–15 min
python eval/setup_online_eval.py     # optional: score live traffic continuously
```
**Check:** `eval/results/<timestamp>.md` exists. If it says "no spans found", wait a few minutes and run
`python eval/run_eval.py --evaluate-only eval/results/<file>.json`.

### Step 10 — Redeploy after a code change

```bash
python deploy/04b_package_and_upload.py && python deploy/05_deploy_runtime.py   # zip path
bash   deploy/04_build_and_push.sh     && python deploy/05_deploy_runtime.py   # Docker path
```

### Step 11 — Clean up (stop paying)

```bash
python deploy/99_cleanup.py --yes
```
Deletes the runtime, online eval, memory, ECR repo, IAM roles and dashboard.
It does not delete the production stack from Part B. Do that first with `sam delete`.

---

## Part B — Production (only after Part A works)

Part A calls the agent with your own AWS credentials. For real users, add the front door
(see `production/README.md`).

### Step 12 — Use the production config

```bash
source deploy/config.prod.env
```
This pins the model, sets the review gate (`REVIEW_PASS_SCORE=7`, `MAX_REVISIONS=1`) and names
the `prod` endpoint. Use it **instead of** `config.env`.

### Step 13 — Deploy a version and promote it to `prod`

```bash
python deploy/05_deploy_runtime.py            # new version; DEFAULT follows it
python deploy/06_invoke.py "smoke test"       # try it on DEFAULT first
python deploy/08_promote_endpoint.py --promote
```
Users hit the `prod` endpoint, which only moves when you promote.

**Check:** `python deploy/08_promote_endpoint.py` (no flags) shows `prod` pointing at the version you expect.

**Roll back in about a second:**
```bash
python deploy/08_promote_endpoint.py --rollback
```

### Step 14 — Deploy the HTTP front door

```bash
cd production
sam build
sam deploy --guided --parameter-overrides \
  AgentRuntimeArn=$(jq -r .agent_runtime_arn ../deploy/state.json) \
  AgentEndpoint=prod \
  UserPoolId=<pool-id> UserPoolClientId=<client-id> NotifyEmail=you@example.com
```
No Cognito pool yet? Follow the "No Cognito pool yet?" snippet in `production/README.md`.

**Check:** `sam deploy` prints an `ApiUrl`. `POST $API/briefs` with an ID token returns `202` and a
`job_id`. Polling `GET $API/briefs/<job_id>` goes QUEUED → RUNNING → SUCCEEDED.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `AccessDeniedException` invoking the model | Enable Claude Sonnet in Bedrock *Model access* and check `MODEL_ID`. |
| `Missing [...] in state.json` | An earlier step didn't finish. Re-run the steps in order. |
| Runtime `CREATE_FAILED` | `aws bedrock-agentcore-control get-agent-runtime --agent-runtime-id <id>` and read `failureReason`. Usually IAM propagation (re-run Step 6) or a non-arm64 image. |
| 500 or `RuntimeClientError` on invoke | `aws logs tail /aws/bedrock-agentcore/runtimes/<id>-DEFAULT --follow` |
| No traces in GenAI Observability | Transaction Search not active yet (Step 2). Wait 10 minutes and invoke again. |
| Memory not recalled | Long-term extraction is async (about 1 minute). Use the same `--actor`. |
| `exec format error` on Docker build | Install arm64 emulation (see Step 5b) or use 5a. |
| Zip path fails to start | `main.py` must be at the zip root, and the role must be able to read the code bucket. Re-run Step 3. |

## Cost note

A few dozen test runs normally cost a few dollars (about 15–30k Bedrock tokens per report).
Run Step 11 when you are done.
