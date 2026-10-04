# Multi-Agent POC on Amazon Bedrock AgentCore

An orchestrator plus three specialist agents (researcher, writer, reviewer) running on
AgentCore Runtime, with memory, evaluations and OpenTelemetry observability — and a
serverless front door so real users can call it without AWS credentials.

## Documentation

Open these in a browser; each is a self-contained HTML page.

| Page | What it covers |
|---|---|
| [`docs/end-to-end-guide.html`](docs/end-to-end-guide.html) | **Start here.** 15 steps from empty account to production, with the failures that actually happened during this build |
| [`docs/data-flow.html`](docs/data-flow.html) | Where data goes, retention, who can read it, how to delete it — written for a security review |
| [`docs/deploy-runbook.html`](docs/deploy-runbook.html) | The deployment steps with console wireframes and a "why" panel per step |
| [`docs/trigger-playbook.html`](docs/trigger-playbook.html) | One use case end to end, then production trigger patterns |
| [`docs/access-request.html`](docs/access-request.html) | What to ask an AWS administrator to enable, ready to forward |

## Layout

```
agent/        the four agents, tools, memory wiring, custom metrics
deploy/       numbered deployment scripts (01 → 08, 99 to tear down)
eval/         evaluation dataset, on-demand runner, online eval setup
production/   SAM stack: HTTP API, queue, worker Lambda, alarms
tests/        offline tests using a scripted fake model — no AWS needed
```

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest
python -m pytest -q tests/        # proves the agent logic with no AWS at all
source deploy/config.env
```

Then follow `docs/end-to-end-guide.html`.

---

## Detailed deployment notes

One **orchestrator** + three **specialist agents** (Strands Agents SDK, Claude Sonnet on Bedrock),
packaged with **Docker**, pushed to **ECR**, hosted on **AgentCore Runtime**, with
**AgentCore Memory**, **AgentCore Evaluations** and **Observability** (CloudWatch / OpenTelemetry).

**POC task – "Research & Report":** the user asks for a report on a topic → the team researches,
writes, quality-reviews (and rewrites if needed) → returns a markdown report.

```
                        ┌──────────────────────────── AgentCore Runtime (microVM per session) ─┐
 invoke_agent_runtime   │                                                                       │
 {"prompt","actor_id"} ─┼─► ORCHESTRATOR (Claude Sonnet) ◄──── AgentCore Memory                 │
                        │     │  plans + delegates           STM: conversation events          │
                        │     ├─► researcher ──► search_knowledge_base tool                     │
                        │     ├─► writer     ──► word_count tool                                │
                        │     └─► reviewer   ──► structured score 0-10 → revise loop (max 1)    │
                        │                                                                       │
                        │  ADOT (OpenTelemetry) ─► CloudWatch: traces/spans, GenAI Observability│
                        │  custom metrics      ─► CloudWatch namespace AgentCoreMultiAgentPOC   │
                        └───────────────────────────────────────────────────────────────────────┘
      AgentCore Evaluations (online: samples live sessions │ on-demand: eval/run_eval.py)
```

| Piece | Where |
|---|---|
| Orchestrator (agents-as-tools pattern) | `agent/orchestrator.py` |
| Researcher / Writer / Reviewer | `agent/specialists.py` |
| Runtime entrypoint (`/invocations`, `/ping`, port 8080) | `agent/main.py` |
| Memory: STM + LTM (preferences, facts, summaries) | `deploy/03_create_memory.py`, `agent/orchestrator.py` |
| Custom metrics (per-agent latency, tokens, review score) | `agent/metrics.py` |
| Evaluations (deterministic + AgentCore built-in judges) | `eval/run_eval.py`, `eval/setup_online_eval.py` |
| Dashboard | `deploy/07_create_dashboard.py` |

---

## Baby steps

Run everything from the project root on **your** machine (Mac/Linux/WSL). ~30–40 min first time.

### Step 0 – One-time prerequisites

1. **Tools**
   ```bash
   aws --version          # AWS CLI v2
   docker buildx version  # Docker Desktop (or docker + buildx)
   python3 --version      # 3.10+
   ```
2. **AWS login** – any method that works for the CLI (SSO, profile, access keys):
   ```bash
   aws sts get-caller-identity       # must print your account id
   ```
   Your user needs admin-ish rights for this POC (IAM, ECR, Bedrock AgentCore, CloudWatch, X-Ray).
3. **Bedrock model access** – Console → *Amazon Bedrock* → *Model access* (us-east-1) → make sure
   **Anthropic Claude Sonnet** is enabled. Check the inference profile id:
   ```bash
   aws bedrock list-inference-profiles --region us-east-1 \
     --query "inferenceProfileSummaries[?contains(inferenceProfileId,'sonnet')].inferenceProfileId"
   ```
   If `us.anthropic.claude-sonnet-4-6` isn't listed, put one that is into `MODEL_ID` in `deploy/config.env`.
4. **Python deps for the scripts**
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt pytest
   ```
5. **(Optional) offline test** – no AWS needed, uses a scripted fake model:
   ```bash
   python -m pytest -q tests/        # 3 passed
   ```

### Step 1 – Load config (every new terminal)
```bash
source deploy/config.env      # prints Account=… Region=us-east-1 Agent=research_orchestrator
```

### Step 2 – Turn on observability (once per account/region)
```bash
bash deploy/01_enable_observability.sh
```
Enables CloudWatch **Transaction Search** – required for traces, the GenAI Observability
dashboard and Evaluations. Takes up to ~10 min to become active (you can continue meanwhile).

### Step 3 – IAM roles
```bash
python deploy/02_create_iam_roles.py
```
Creates `multiagent-poc-runtime-role` (used by the container) and `multiagent-poc-eval-role`
(used by online evaluations). IDs are saved to `deploy/state.json`.

### Step 4 – Memory
```bash
python deploy/03_create_memory.py      # ~2-3 min, prints MEMORY_ID=...
```

### Step 5 – Package the agent (pick one)

**5a · No Docker (works in AWS CloudShell) — recommended if you don't have Docker**
```bash
python deploy/04b_package_and_upload.py
```
Downloads linux/arm64 wheels, zips them with `agent/` and a `main.py` entry point (~39 MB),
creates `s3://bedrock-agentcore-code-<account>-<region>/` and uploads. No image, no ECR,
no arm64 emulation. Step 6 then deploys the zip automatically.

**5b · Docker build → ECR**
```bash
bash deploy/04_build_and_push.sh
```
Builds a **linux/arm64** image (required by AgentCore) and pushes `…/multiagent-poc-agent:<tag>`.
On Intel/AMD machines buildx emulates arm64 – slower but fine. If it errors with
`exec format error`, run once: `docker run --privileged --rm tonistiigi/binfmt --install arm64`.

### Step 6 – Deploy to AgentCore Runtime
```bash
python deploy/05_deploy_runtime.py     # waits until READY (1-3 min)
```
It deploys whichever artifact you built last — the S3 zip if you used 5a, the ECR image if you
used 5b. Force either with `--code` / `--container`.

### Step 7 – Trigger it 🎯
```bash
python deploy/06_invoke.py "Write a short report on AgentCore Memory for backend engineers"
```
You'll get the report plus a per-agent breakdown (latency + tokens) and the reviewer score.

**Test memory:**
```bash
# 1) tell it a preference
python deploy/06_invoke.py --actor alice "For all my reports: I'm a CFO, plain language, under 150 words."
# 2) wait ~1-2 min for long-term memory extraction, then use a NEW session (no --session)
python deploy/06_invoke.py --actor alice "Write me a report on AgentCore Runtime."
#    -> should be short and jargon-free: the preference came from long-term memory
# 3) same session = short-term memory
python deploy/06_invoke.py --actor alice --session <session id printed above> "What did I ask you earlier?"
```

Other ways to trigger: AWS Console → *Bedrock AgentCore* → *Agent runtime* → your agent → **Test**
(payload `{"prompt": "...", "actor_id": "alice"}`), or the CLI:
```bash
aws bedrock-agentcore invoke-agent-runtime --agent-runtime-arn "$(jq -r .agent_runtime_arn deploy/state.json)" \
  --runtime-session-id "cli-session-$(uuidgen)" --payload '{"prompt":"Explain AgentCore in 5 bullets"}' \
  --cli-binary-format raw-in-base64-out out.json && jq -r .result out.json
```

### Step 8 – Observability dashboard
```bash
python deploy/07_create_dashboard.py   # prints two console links
```
* **GenAI Observability** (CloudWatch → *GenAI Observability* → *Bedrock AgentCore*): sessions,
  traces with the full span tree orchestrator → tool → sub-agent → model call, token usage,
  latency, errors. Built-in, from ADOT.
* **Custom dashboard** `multiagent-poc-dashboard`: per-agent latency & tokens, reviewer score,
  revision count, eval scores, runtime invocations/errors.
* **Logs:** `/aws/bedrock-agentcore/runtimes/<runtime-id>-DEFAULT`

### Step 9 – Evaluation
**a) Batch / regression eval (on-demand):**
```bash
python eval/run_eval.py                 # 4 test cases, ~10-15 min incl. waiting for traces
```
For each case it invokes the agent, runs deterministic checks (keywords, word limit, headings,
sources, reviewer approval), then asks **AgentCore Evaluations** to judge the real trace with
`GoalSuccessRate` (against the case's assertions), `Helpfulness`, `Faithfulness`,
`ToolSelectionAccuracy` and `TrajectoryInOrderMatch` (expected researcher → writer → reviewer).
Output: `eval/results/<timestamp>.md` + `.json`, and `EvalScore` metrics on the dashboard.
If traces weren't ready: `python eval/run_eval.py --evaluate-only eval/results/<file>.json`.
Add your own cases in `eval/dataset.json`.

**b) Continuous (online) eval on live traffic:**
```bash
python eval/setup_online_eval.py        # samples 100% of sessions (POC setting)
```
Scores appear in GenAI Observability → your agent → **Evaluations** a few minutes after a session
goes idle (15 min). Pause with `--disable`.

### Step 10 – Change code & redeploy
```bash
python deploy/04b_package_and_upload.py && python deploy/05_deploy_runtime.py   # zip path
bash   deploy/04_build_and_push.sh     && python deploy/05_deploy_runtime.py   # container path
```

### Step 11 – Clean up (stop paying)
```bash
python deploy/99_cleanup.py --yes
```

---

## Running it all in AWS CloudShell

CloudShell is already signed in as your console identity, so there is no `aws configure` step and
no access keys.

1. Open CloudShell (the terminal icon in the console top bar), region **us-east-1**.
2. **Actions → Upload file** → `agentcore-multiagent-poc.zip`, then:
   ```bash
   unzip agentcore-multiagent-poc.zip && cd agentcore-multiagent-poc
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Run steps 1–11 as written, using **5a** (the zip path) rather than the Docker build —
   CloudShell is x86 and cannot easily cross-build arm64 images.

Notes: home is 1 GB (the build directory is deleted and rebuilt each time, so keep an eye on it);
idle sessions are reclaimed, so re-source `deploy/config.env` and re-activate the venv when you
come back. `deploy/state.json` lives in your home directory and survives.

---

## Troubleshooting
| Symptom | Fix |
|---|---|
| `AccessDeniedException` invoking model | Enable Claude Sonnet in Bedrock *Model access*; check `MODEL_ID`. |
| Runtime `CREATE_FAILED` | `aws bedrock-agentcore-control get-agent-runtime --agent-runtime-id <id>` → `failureReason`. Usually IAM propagation (re-run step 6) or a non-arm64 image. |
| 500 / `RuntimeClientError` on invoke | Check logs: `aws logs tail /aws/bedrock-agentcore/runtimes/<id>-DEFAULT --follow`. |
| No traces in GenAI Observability | Transaction Search not active yet (step 2) – wait 10 min, invoke again. |
| run_eval: "no spans found" | Traces take a few minutes; re-run with `--evaluate-only`. |
| Memory not recalled across sessions | LTM extraction is async (~1 min+). Same `--actor` must be used. |
| `exec format error` during docker build | Install arm64 emulation once (`docker run --privileged --rm tonistiigi/binfmt --install arm64`), or switch to step 5a. |
| Runtime fails to start on the zip path | Check `main.py` sits at the zip root and the execution role can read the code bucket (re-run step 3). |

## Cost notes (POC scale)
Runtime bills per active CPU-second/GB-second; Memory per event/record; Evaluations per judged
token; Bedrock per token (5 model calls per report, ~15-30k tokens). A few dozen test runs is
typically a few dollars. Run step 11 when finished.
