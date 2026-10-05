# AgentCore + GitHub Actions: Complete Command Playbook

Every command and file needed to: set up a machine, bootstrap AWS, deploy an agent to Amazon Bedrock
AgentCore Runtime, invoke and monitor it, evaluate it, and deploy it automatically from GitHub
Actions (PR -> dev, merge -> prod) with no AWS keys stored in GitHub.

Written from the `agentcore-poc` build, but parameterised so it can be reused in another project:
change the variables in section 0 and the placeholders in the appendices.

Contents: 0 Variables · 1 Machine setup · 2 AWS bootstrap · 3 Deploy · 4 Invoke · 5 Local monitor ·
6 Logs, dashboard, evaluation · 7 GitHub Actions · 8 Clean up · 9 Reuse checklist · 10 Troubleshooting ·
Appendix A workflow · B IAM policies · C CI state helper

---

## 0. Variables (set once per terminal; edit for a new project)

```bash
export ACCOUNT_ID=770457183855            # aws sts get-caller-identity --query Account --output text
export REGION=us-east-1
export GH_OWNER=dhilipansankaralingam-del # GitHub user or org
export GH_REPO=strands_etl
export PROJECT=multiagent-poc             # prefix for roles / memory / ECR
export PROD_AGENT=research_orchestrator   # prod runtime name (letters, digits, underscore only)
export DEV_AGENT=research_orchestrator_dev
export DEPLOY_ROLE=strands-etl-github-deploy   # IAM role GitHub Actions assumes
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION
```
In this repo `source deploy/config.env` sets the AWS-side ones (`PROJECT`, `AGENT_NAME`, `MODEL_ID`, ...) and
`source deploy/config.prod.env` sets production values (pinned model, review gate, `ENDPOINT_NAME=prod`).

| Name | What it is |
|---|---|
| `$DEV_AGENT` | Dev runtime. `DEFAULT` endpoint = newest version. Deployed by every PR. |
| `$PROD_AGENT` | Prod runtime. Its `prod` endpoint is moved only by a merge to `main`. |
| `$DEPLOY_ROLE` | OIDC role GitHub Actions assumes. |
| `${PROJECT}-runtime-role`, `${PROJECT}-eval-role` | Runtime / evaluation roles (created by `02_create_iam_roles.py`). |
| `${PROJECT//-/_}_memory-<suffix>` | AgentCore Memory, shared by dev and prod. |

---

## 1. Machine setup (Windows; Git Bash or PowerShell)

```powershell
winget install --id GitHub.cli -e
winget install --id Amazon.AWSCLI -e
```
Open a **new** terminal afterwards. Full paths if PATH isn't refreshed:
`C:\Program Files\GitHub CLI\gh.exe`, `C:\Program Files\Amazon\AWSCLIV2\aws.exe`.
Git Bash: `export PATH="$PATH:/c/Program Files/Amazon/AWSCLIV2"` and `export MSYS_NO_PATHCONV=1` (stops it rewriting `/aws/...` log-group paths).

### GitHub login (stored in the Windows credential store)
```bash
gh auth login --with-token                 # paste a token with the repo scope (or: gh auth login -w -h github.com -p https)
gh auth refresh -h github.com -s workflow  # REQUIRED to push anything under .github/workflows/
gh auth setup-git                          # make git use the same login
gh auth status
```

### AWS login (stored in ~/.aws)
```bash
aws configure sso          # IAM Identity Center, or:
aws configure              # access keys; region $REGION; output json
aws sts get-caller-identity   # confirm the account id
```
> Use an IAM user or SSO role, not root. Never paste a secret key into chat, tickets or commits.
> Enable the model once: Console -> Bedrock -> Model access -> Anthropic Claude Sonnet (needed to invoke, not to deploy).

### Python environment
```bash
python -m venv .venv && source .venv/Scripts/activate     # Windows Git Bash (Linux/Mac: .venv/bin/activate)
pip install -r requirements.txt pytest
python -m pytest -q tests/                                # offline tests, no AWS needed
export PYTHONUTF8=1                                       # prevents UnicodeEncodeError on the Windows console
```

---

## 2. One-time AWS bootstrap

```bash
source deploy/config.env
bash   deploy/01_enable_observability.sh    # CloudWatch Transaction Search (once per account+region)
python deploy/02_create_iam_roles.py        # runtime + eval roles (idempotent)
python deploy/03_create_memory.py           # AgentCore Memory (idempotent; reuses an existing one)
```

### GitHub OIDC trust (GitHub Actions deploys without AWS keys)
Policy files: Appendix B (`deploy/github-oidc/*.json`). Review them first: the role trusts only
`repo:$GH_OWNER/$GH_REPO` pull requests and `main`.
```bash
aws iam list-open-id-connect-providers                    # create only if token.actions.githubusercontent.com is absent
aws iam create-open-id-connect-provider --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
aws iam create-role --role-name $DEPLOY_ROLE \
  --assume-role-policy-document file://deploy/github-oidc/trust-policy.json
aws iam put-role-policy --role-name $DEPLOY_ROLE --policy-name agentcore-deploy \
  --policy-document file://deploy/github-oidc/permissions-policy.json
aws iam get-role --role-name $DEPLOY_ROLE --query Role.Arn --output text     # verify
```

---

## 3. Deploy

### Manual (the same steps CI runs)
```bash
source deploy/config.env
python deploy/04b_package_and_upload.py     # linux/arm64 zip -> s3://bedrock-agentcore-code-$ACCOUNT_ID-$REGION/<agent>/
python deploy/05_deploy_runtime.py --code   # create or update the runtime, wait for READY (1-3 min)
```
Dev vs prod is only `AGENT_NAME`:
```bash
AGENT_NAME=$DEV_AGENT  python deploy/05_deploy_runtime.py --code
AGENT_NAME=$PROD_AGENT python deploy/05_deploy_runtime.py --code
```
Docker/ECR route instead of the zip: `bash deploy/04_build_and_push.sh && python deploy/05_deploy_runtime.py --container`
(arm64 image; `docker run --privileged --rm tonistiigi/binfmt --install arm64` if you hit `exec format error`).
Switching between zip and container on an existing runtime needs `--recreate`.

On a fresh checkout `deploy/state.json` is missing; rebuild it from AWS (Appendix C): `python deploy/ci_bootstrap_state.py`

### Promote / roll back the prod endpoint
```bash
source deploy/config.prod.env
python deploy/08_promote_endpoint.py             # show versions and where `prod` points
python deploy/08_promote_endpoint.py --promote   # prod -> newest version
python deploy/08_promote_endpoint.py --rollback  # prod -> previous version (about 1 second)
python deploy/08_promote_endpoint.py --promote --version 3   # a specific version
```

### Inspect
```bash
aws bedrock-agentcore-control list-agent-runtimes \
  --query "agentRuntimes[].[agentRuntimeName,status,agentRuntimeVersion]" --output text
aws bedrock-agentcore-control get-agent-runtime --agent-runtime-id <runtime-id> --query "[status,failureReason]"
aws bedrock-agentcore-control get-agent-runtime-endpoint --agent-runtime-id <runtime-id> \
  --endpoint-name prod --query "[name,status,liveVersion]" --output text
aws bedrock-agentcore-control list-agent-runtime-endpoints --agent-runtime-id <runtime-id>
```

---

## 4. Invoke the agent

`deploy/06_invoke.py` reads the target from `deploy/state.json`. Point it at a runtime (strip `\r`, Windows adds it):
```bash
read ID ARN < <(aws bedrock-agentcore-control list-agent-runtimes \
  --query "agentRuntimes[?agentRuntimeName=='$DEV_AGENT'].[agentRuntimeId,agentRuntimeArn]" --output text | tr -d '\r')
python -c "import sys; sys.path.insert(0,'deploy'); from common import save_state; save_state(agent_runtime_id='$ID', agent_runtime_arn='$ARN')"
```
```bash
python deploy/06_invoke.py --actor demo "Write a short report on AgentCore Memory for backend engineers"
python deploy/06_invoke.py --actor demo --endpoint prod "..."          # the prod endpoint
python deploy/06_invoke.py --actor demo --session <session-id> "..."   # same session = short-term memory
python deploy/06_invoke.py --raw "..."                                  # raw JSON
```
A run (research -> write -> review) takes 40-90 s. Output: report, reviewer score, revisions, per-agent latency and tokens.
Long-term memory: use the same `--actor` in a **new** session, about 1-2 min after telling it a preference.

Plain AWS CLI:
```bash
aws bedrock-agentcore invoke-agent-runtime --agent-runtime-arn "<arn>" --qualifier prod \
  --runtime-session-id "cli-session-$(uuidgen)" --payload '{"prompt":"Explain AgentCore in 5 bullets","actor_id":"demo"}' \
  --cli-binary-format raw-in-base64-out out.json && jq -r .result out.json
```
Payload contract: `{"prompt": "...", "actor_id": "..."}`; qualifier `DEFAULT` = newest version, `prod` = promoted version.

---

## 5. Local monitor page

`tools/monitor.py` (Python standard library + boto3). Invokes the agent and shows the report, review score,
per-agent latency/token bars, runtime and endpoint status, run history and live CloudWatch logs.
It binds to `127.0.0.1` only and uses your saved AWS login, so anyone at that browser can call the agent.
```bash
python tools/monitor.py                                      # http://localhost:8000  dev + prod
MONITOR_ENV=prod MONITOR_PORT=8001 python tools/monitor.py   # http://localhost:8001  prod only
```
Stop it: Ctrl+C, or in PowerShell
`Get-NetTCPConnection -LocalPort 8000,8001 -State Listen | % { Stop-Process -Id $_.OwningProcess -Force }`
Don't refresh the tab mid-run: the run continues in AWS but the result is lost.
Runtime names are in the `RUNTIMES` dict at the top of the file; change them for another project.

---

## 6. Logs, dashboard, evaluation

### Logs (dev logs: `...-DEFAULT`; the prod endpoint writes to its own `...-prod` group)
```bash
aws logs describe-log-groups --log-group-name-prefix /aws/bedrock-agentcore/runtimes/ --query "logGroups[].logGroupName"
aws logs tail /aws/bedrock-agentcore/runtimes/<dev-runtime-id>-DEFAULT --follow
aws logs tail /aws/bedrock-agentcore/runtimes/<prod-runtime-id>-prod --follow
# invocations in the last hour
aws logs filter-log-events --log-group-name <group> --start-time $(( ($(date +%s)-3600)*1000 )) --filter-pattern '"invoke session"'
```

### CloudWatch dashboard and traces
```bash
python deploy/07_create_dashboard.py      # creates <PROJECT>-dashboard
```
- `https://$REGION.console.aws.amazon.com/cloudwatch/home?region=$REGION#dashboards/dashboard/multiagent-poc-dashboard`
- `https://$REGION.console.aws.amazon.com/cloudwatch/home?region=$REGION#gen-ai-observability/agent-core`

### Evaluation
```bash
python eval/run_eval.py                                            # 4 cases, 10-15 min -> eval/results/<ts>.md and .json
python eval/run_eval.py --evaluate-only eval/results/<file>.json   # re-judge after traces have landed
python eval/setup_online_eval.py                                   # continuous eval on live traffic (--disable to pause)
```
Cases live in `eval/dataset.json`. The judge columns (GoalSuccessRate, Helpfulness, ...) stay blank if spans never reached
CloudWatch (see Troubleshooting).

---

## 7. GitHub Actions: PR -> dev, merge -> prod

Workflow file: Appendix A (`.github/workflows/agentcore-deploy.yml`).

| Event | Jobs | Result |
|---|---|---|
| Pull request touching `agentcore-poc/**` | `test` then `deploy` | Deploys `$DEV_AGENT`. Skipped for fork PRs. |
| Push or merge to `main` touching `agentcore-poc/**` | `test` then `deploy` | Deploys `$PROD_AGENT` and runs `08_promote_endpoint.py --promote`. |

- **Auth:** `permissions: id-token: write` plus `aws-actions/configure-aws-credentials@v4` assumes `$DEPLOY_ROLE`. No AWS secrets in GitHub.
- **Do not** add `environment:` to the job. It changes the OIDC `sub` claim and the trust policy stops matching.
- **Concurrency:** one deploy per environment at a time; dev and prod groups are separate.
- **Shared state:** CI has no `state.json`, so `ci_bootstrap_state.py` looks up the role and memory by name (Appendix C).

### Day-to-day
```bash
git checkout -b my-change
# edit files under agentcore-poc/ ...
git add -A && git commit -m "..." && git push -u origin my-change
gh pr create --base main --head my-change --title "..." --body "..."   # opens the PR, starts the dev deploy
gh pr checks <pr-number>                                               # test + deploy status
gh run list --branch my-change --limit 3
gh run view <run-id> --log-failed                                      # why a run failed
gh pr merge <pr-number> --merge                                        # merge, starts the prod deploy
gh run list --branch main --limit 2
```
Confirm after a merge:
```bash
aws bedrock-agentcore-control get-agent-runtime-endpoint --agent-runtime-id <prod-runtime-id> \
  --endpoint-name prod --query "[name,status,liveVersion]" --output text
```
Adding commits to an already-open PR: push to the same branch; the workflow reruns and redeploys dev.
A PR that is already merged cannot take more commits: open a new branch and PR.

---

## 8. Clean up

```bash
python deploy/99_cleanup.py --yes      # runtime(s), online eval, memory, ECR repo, IAM roles, dashboard
aws bedrock-agentcore-control delete-agent-runtime --agent-runtime-id <dev-runtime-id>   # dev only
# remove the GitHub deploy role + provider
aws iam delete-role-policy --role-name $DEPLOY_ROLE --policy-name agentcore-deploy
aws iam delete-role --role-name $DEPLOY_ROLE
aws iam delete-open-id-connect-provider \
  --open-id-connect-provider-arn arn:aws:iam::$ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com
```
Empty and delete `s3://bedrock-agentcore-code-$ACCOUNT_ID-$REGION` separately if you want it gone.

---

## 9. Reuse checklist (new project / new account)

1. Copy `deploy/`, `agent/`, `eval/`, `tools/monitor.py`, `.github/workflows/agentcore-deploy.yml`, `deploy/github-oidc/`.
2. Section 0: set your variables. In the files, replace the placeholders from Appendix A-B
   (`<ACCOUNT_ID>`, `<REGION>`, `<GH_OWNER>/<GH_REPO>`, `<DEPLOY_ROLE>`, `<PROJECT>`) and the runtime names (`AGENT_NAME` in the workflow, `RUNTIMES` in `monitor.py`).
3. Section 1: tools, logins (`workflow` scope), venv.
4. Section 2: observability, roles, memory, OIDC provider + role (create the role **before** the first PR).
5. Section 3: run one manual deploy to make sure the code works.
6. Push a branch and open a PR: the first dev deploy proves the whole pipeline. Merge to get the prod endpoint.
7. If the repo path differs, update `paths:` and every `working-directory:` in the workflow.
8. Replace root keys with IAM-user or SSO credentials, and delete any root access keys.

---

## 10. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `gh` / `aws` not found in an already-open terminal | Open a new terminal or use the full `.exe` path. |
| `git push` rejected: "without `workflow` scope" | `gh auth refresh -h github.com -s workflow`. |
| Actions fails: "Not authorized to perform sts:AssumeRoleWithWebIdentity" | Trust policy `sub` doesn't match the event (PR vs `main`), wrong repo name, or the job uses `environment:`. |
| `AccessDeniedException` on invoke | Enable the model in Bedrock Model access; check `MODEL_ID`. |
| Runtime `CREATE_FAILED` | `get-agent-runtime ... failureReason`. Usually IAM propagation (re-run) or a non-arm64 artifact. |
| `UnicodeEncodeError: 'charmap'` printing the report | `export PYTHONUTF8=1`. The agent call had actually succeeded. |
| `aws ... --output text` breaks a Python string | Windows adds `\r`; pipe through `tr -d '\r'`. |
| Monitor log panel empty for prod | Prod logs are in `...-prod`, not `...-DEFAULT`. |
| `Failed to export span batch code: 400` in logs | Traces may not reach CloudWatch; GenAI Observability and the evaluation judge scores stay empty. Investigate the OTLP exporter / Transaction Search. |
| Memory not recalled across sessions | Long-term extraction is async (~1 min); use the same `--actor`. |
| Dev and prod share one memory | Don't put real user data through dev. |
| Claude Code blocks IAM / OIDC commands | Creating roles and trust is a protected action; run them yourself or allow them explicitly. |
| Machine runs out of memory | Don't run the monitor, an evaluation and a build at the same time on a small machine. |

---

## Appendix A: `.github/workflows/agentcore-deploy.yml` (template)

```yaml
name: agentcore-deploy

# PR opened/updated -> tests + deploy to the DEV runtime (research_orchestrator_dev)
# merge to main     -> tests + deploy to the PROD runtime and move the `prod` endpoint
on:
  pull_request:
    paths: ["agentcore-poc/**", ".github/workflows/agentcore-deploy.yml"]
  push:
    branches: [main]
    paths: ["agentcore-poc/**", ".github/workflows/agentcore-deploy.yml"]

permissions:
  id-token: write   # OIDC token for AWS - no stored AWS keys
  contents: read

# One deploy per environment at a time; never cancel one mid-flight.
concurrency:
  group: agentcore-${{ github.event_name == 'push' && 'prod' || 'dev' }}
  cancel-in-progress: false

env:
  AWS_REGION: <REGION>
  AWS_DEFAULT_REGION: <REGION>
  ROLE_ARN: arn:aws:iam::<ACCOUNT_ID>:role/<DEPLOY_ROLE>
  PROJECT: <PROJECT>

jobs:
  test:
    runs-on: ubuntu-latest
    defaults: {run: {working-directory: agentcore-poc}}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.13"}
      - run: pip install -r requirements.txt pytest
      - run: python -m pytest -q tests/

  deploy:
    needs: test
    # Fork PRs get no OIDC token and must never reach the AWS account.
    if: github.event_name == 'push' || github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    defaults: {run: {working-directory: agentcore-poc}}
    env:
      AGENT_NAME: ${{ github.event_name == 'push' && 'research_orchestrator' || 'research_orchestrator_dev' }}
      ENDPOINT_NAME: prod
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.13"}
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ env.ROLE_ARN }}
          aws-region: ${{ env.AWS_REGION }}
      - run: pip install -r requirements.txt
      - name: Load role + memory IDs from AWS
        run: python deploy/ci_bootstrap_state.py
      - name: Package and upload zip
        run: python deploy/04b_package_and_upload.py
      - name: Deploy runtime ($AGENT_NAME)
        run: python deploy/05_deploy_runtime.py --code
      - name: Promote prod endpoint (main only)
        if: github.event_name == 'push'
        run: python deploy/08_promote_endpoint.py --promote
      - name: Summary
        run: |
          echo "### Deployed \`$AGENT_NAME\`" >> "$GITHUB_STEP_SUMMARY"
          jq -r '"- runtime: \(.agent_runtime_arn)\n- version: \(.agent_runtime_version)"' deploy/state.json >> "$GITHUB_STEP_SUMMARY"
```

## Appendix B: IAM policies (`deploy/github-oidc/`)

`trust-policy.json`, who may assume the role:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"},
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
      "StringLike": {"token.actions.githubusercontent.com:sub": [
        "repo:<GH_OWNER>/<GH_REPO>:pull_request",
        "repo:<GH_OWNER>/<GH_REPO>:ref:refs/heads/main"
      ]}
    }
  }]
}
```
`permissions-policy.json`, what the role may do:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "AgentCoreControlPlane", "Effect": "Allow",
     "Action": ["bedrock-agentcore:*"], "Resource": "*"},
    {"Sid": "CodeBucket", "Effect": "Allow",
     "Action": ["s3:CreateBucket", "s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject", "s3:PutObject"],
     "Resource": ["arn:aws:s3:::bedrock-agentcore-code-<ACCOUNT_ID>-<REGION>",
                  "arn:aws:s3:::bedrock-agentcore-code-<ACCOUNT_ID>-<REGION>/*"]},
    {"Sid": "ReadRuntimeRole", "Effect": "Allow",
     "Action": ["iam:GetRole"], "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/<PROJECT>-runtime-role"},
    {"Sid": "PassRuntimeRole", "Effect": "Allow",
     "Action": ["iam:PassRole"], "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/<PROJECT>-runtime-role",
     "Condition": {"StringEquals": {"iam:PassedToService": "bedrock-agentcore.amazonaws.com"}}}
  ]
}
```
The OIDC provider must exist first (section 2). Add a second `sub` entry if you also deploy from a tag or another branch.

## Appendix C: `deploy/ci_bootstrap_state.py`

Rebuilds the git-ignored `deploy/state.json` in CI by looking up the runtime role and memory by name. It creates nothing.
```python
"""CI helper - rebuild deploy/state.json from AWS.

The deploy scripts share IDs through deploy/state.json, which is git-ignored, so a
fresh CI checkout starts without it. The role and memory are created once by
02_create_iam_roles.py / 03_create_memory.py; this looks them up by name.
Nothing is created here.
"""
import sys

from common import PROJECT, control, load_state, save_state
import boto3

role_name = f"{PROJECT}-runtime-role"
memory_prefix = f"{PROJECT.replace('-', '_')}_memory"

role_arn = boto3.client("iam").get_role(RoleName=role_name)["Role"]["Arn"]

memory_id = next((m["id"] for m in control().list_memories().get("memories", [])
                  if m["id"].startswith(memory_prefix)), None)
if not memory_id:
    sys.exit(f"No memory named {memory_prefix}* found. Run deploy/03_create_memory.py once first.")

save_state(runtime_role_arn=role_arn, memory_id=memory_id)
print(f"state.json: runtime_role_arn={role_arn} memory_id={memory_id} ({list(load_state())})")
```
