# Setup Baby Steps: from a blank machine to auto-deploy on every PR

The whole setup in order: tools, logins, AWS bootstrap, deploy, invoke, monitor, and GitHub Actions
(PR -> dev runtime, merge -> prod runtime). Do the steps in order. After each step there is a **Check**:
do not move on until it passes.

- All the commands in one place, with variables and the template files: `docs/command-reference.md`
- The deployment-only walkthrough: `deployment-baby-steps.md`

Time: about 1 hour the first time (most of it waiting for AWS). Windows with Git Bash is assumed;
Linux/Mac differences are noted.

Values used here: account `770457183855`, region `us-east-1`, repo `dhilipansankaralingam-del/strands_etl`.
Replace them with yours (see section 0 of `docs/command-reference.md`).

---

## Phase 1: Your machine

### Step 1: Install the tools
```powershell
winget install --id GitHub.cli -e
winget install --id Amazon.AWSCLI -e
python --version        # 3.10 or newer; install from python.org if missing
git --version
```
Open a **new** terminal afterwards.

**Check:** `gh --version` and `aws --version` both print a version. If not, use the full paths
`C:\Program Files\GitHub CLI\gh.exe` and `C:\Program Files\Amazon\AWSCLIV2\aws.exe`.

### Step 2: Log in to GitHub (once; saved in the Windows credential store)
1. Create a token at github.com/settings/tokens/new with the `repo` scope.
2. Run `gh auth login --with-token` and paste it (or `gh auth login -w -h github.com -p https` for the browser flow).
3. Add the scope needed to push workflow files, and let git reuse the login:
   ```bash
   gh auth refresh -h github.com -s workflow     # follow the one-time code in the browser
   gh auth setup-git
   ```

**Check:** `gh auth status` shows your account and the scopes `repo` and `workflow`.

### Step 3: Log in to AWS (once; saved in `C:\Users\<you>\.aws`)
- SSO: `aws configure sso`
- or access keys: `aws configure` (region `us-east-1`, output `json`). Keys come from IAM -> Users -> Security credentials.

Use an IAM user or SSO role, **not the root account**. Never paste a secret key into chat or commit it.

**Check:** `aws sts get-caller-identity` prints your account id, and the ARN is **not** `...:root`.

### Step 4: Turn on the model (once)
Console -> *Amazon Bedrock* -> *Model access* (us-east-1) -> enable **Anthropic Claude Sonnet**.

**Check:**
```bash
aws bedrock list-inference-profiles --region us-east-1 \
  --query "inferenceProfileSummaries[?contains(inferenceProfileId,'sonnet')].inferenceProfileId"
```
lists `us.anthropic.claude-sonnet-4-6`. If it lists a different id, put that into `MODEL_ID` in `deploy/config.env`.

### Step 5: Get the code and install dependencies
```bash
git clone https://github.com/dhilipansankaralingam-del/strands_etl.git
cd strands_etl/agentcore-poc
python -m venv .venv && source .venv/Scripts/activate     # Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt pytest
export PYTHONUTF8=1                                       # stops Windows console encoding crashes
python -m pytest -q tests/
```
**Check:** the tests pass. They use a fake model and need no AWS.

---

## Phase 2: AWS, one-time

### Step 6: Load the config (every new terminal)
```bash
source deploy/config.env
```
**Check:** it prints `Account=770457183855 Region=us-east-1 Agent=research_orchestrator`.

### Step 7: Turn on observability
```bash
bash deploy/01_enable_observability.sh
```
**Check:** it ends with `Transaction Search is ON`. (Without it there are no traces and evaluations fail.)

### Step 8: Create the roles and the memory
```bash
python deploy/02_create_iam_roles.py
python deploy/03_create_memory.py
```
**Check:** it prints both role ARNs and `MEMORY_ID=...`. Both scripts are safe to re-run.

### Step 9: Create the GitHub-to-AWS trust (this is what lets Actions deploy with no stored keys)
Open `deploy/github-oidc/trust-policy.json` and check that the repo name is yours. It limits who can assume the role to
this repo's pull requests and `main`. Then:
```bash
aws iam list-open-id-connect-providers          # skip the next command if token.actions.githubusercontent.com is listed
aws iam create-open-id-connect-provider --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
aws iam create-role --role-name strands-etl-github-deploy \
  --assume-role-policy-document file://deploy/github-oidc/trust-policy.json
aws iam put-role-policy --role-name strands-etl-github-deploy --policy-name agentcore-deploy \
  --policy-document file://deploy/github-oidc/permissions-policy.json
```
**Check:** `aws iam get-role --role-name strands-etl-github-deploy --query Role.Arn --output text` prints the role ARN.
Do this **before** your first PR, or the first deploy run will fail.

---

## Phase 3: Deploy and try it by hand

### Step 10: Deploy the dev runtime
```bash
AGENT_NAME=research_orchestrator_dev python deploy/04b_package_and_upload.py
AGENT_NAME=research_orchestrator_dev python deploy/05_deploy_runtime.py --code
```
It takes 1-3 minutes.

**Check:** it prints `READY arn=...`, and
`aws bedrock-agentcore-control list-agent-runtimes --query "agentRuntimes[].[agentRuntimeName,status]" --output text`
shows `research_orchestrator_dev READY`.

### Step 11: Invoke it
```bash
python deploy/06_invoke.py --actor demo "Write a short report on AgentCore Memory for backend engineers"
```
A run takes 40-90 seconds.

**Check:** you get a markdown report with sources, a review score (7 or above passes), and a per-agent latency/token table.

**Memory test (optional):** tell it a preference with `--actor alice`, wait 1-2 minutes, then ask again in a **new** session with the
same `--actor`. The answer should follow the preference.

### Step 12: Open the local monitor page
```bash
python tools/monitor.py                                      # http://localhost:8000  dev + prod
MONITOR_ENV=prod MONITOR_PORT=8001 python tools/monitor.py   # http://localhost:8001  prod only
```
Type a prompt, click **Run**, and don't refresh the tab until it finishes.

**Check:** the page shows the report, the score, per-agent bars and a status of `READY` for the runtime. The log panel shows
events. Stop the server with Ctrl+C when done.

### Step 13: Create the CloudWatch dashboard
```bash
python deploy/07_create_dashboard.py
```
**Check:** it prints two console links. Open `multiagent-poc-dashboard` in CloudWatch; per-agent latency and tokens appear after a run.

### Step 14: Run the evaluation (optional, about 15 minutes)
It runs against whichever runtime `deploy/state.json` points at (the one Step 10 just deployed).
```bash
python eval/run_eval.py
```
**Check:** `eval/results/<timestamp>.md` exists with a row per case. If the judge columns are blank, traces were not exported yet:
see Troubleshooting, then re-run with `--evaluate-only eval/results/<file>.json`.

---

## Phase 4: Automatic deploys with GitHub Actions

What you built in Step 9 plus the workflow file `.github/workflows/agentcore-deploy.yml` gives you:

| You do this | GitHub does this |
|---|---|
| Open or update a PR touching `agentcore-poc/**` | Runs the tests, then deploys `research_orchestrator_dev` |
| Merge to `main` | Runs the tests, deploys `research_orchestrator`, moves the `prod` endpoint |

### Step 15: Open a PR and watch the dev deploy
```bash
git checkout -b my-first-change
# make a small edit under agentcore-poc/, for example in a doc
git add -A && git commit -m "My first change" && git push -u origin my-first-change
gh pr create --base main --head my-first-change --title "My first change" --body "Testing the pipeline"
gh pr checks <pr-number>
```
**Check:** both `test` and `deploy` show `pass`, and the dev runtime is still `READY` (its version number goes up by one).

If `deploy` fails with "Not authorized to perform sts:AssumeRoleWithWebIdentity", the role from Step 9 is missing or its trust
policy names the wrong repo. If the push is rejected for the `workflow` scope, redo Step 2.

### Step 16: Merge and check prod
```bash
gh pr merge <pr-number> --merge
gh run list --branch main --limit 2
aws bedrock-agentcore-control get-agent-runtime-endpoint --agent-runtime-id <prod-runtime-id> \
  --endpoint-name prod --query "[name,status,liveVersion]" --output text
```
The prod runtime id comes from `aws bedrock-agentcore-control list-agent-runtimes`.

**Check:** the `main` run is `success` and the `prod` endpoint is `READY` on the newest version.

### Step 17: Try prod from the browser
```bash
MONITOR_ENV=prod MONITOR_PORT=8001 python tools/monitor.py
```
Open http://localhost:8001, send a prompt.

**Check:** you get a report. These calls are real prod traffic and cost tokens. Prod logs are in the group ending `-prod`.

### Step 18: Practise a rollback (30 seconds, do it once before you need it)
```bash
source deploy/config.prod.env
python deploy/08_promote_endpoint.py             # shows the versions and where prod points
python deploy/08_promote_endpoint.py --rollback  # prod goes back one version
python deploy/08_promote_endpoint.py --promote   # and forward again
```
**Check:** the script reports the `prod` endpoint on the version you expected after each command.

---

## Phase 5: Tidy up and harden

### Step 19: Remove root access from your machine
If Step 3 used root keys: in IAM create a user (or SSO role) with the rights you need, create its access key, run `aws configure`
with the new key, then **delete the root access keys** (account menu -> Security credentials).

**Check:** `aws sts get-caller-identity` no longer shows `:root`.

### Step 20: Stop paying when you are finished
```bash
python deploy/99_cleanup.py --yes
```
Removes the runtime(s), memory, ECR repo, eval and runtime roles, and the dashboard. It does not remove the GitHub deploy role or the OIDC
provider; the commands for that are in section 8 of `docs/command-reference.md`.

---

## Quick troubleshooting

| Symptom | Fix |
|---|---|
| `gh` or `aws` not found | Open a new terminal, or use the full `.exe` path. |
| `UnicodeEncodeError` printing the report | `export PYTHONUTF8=1`. The agent call had worked. |
| `Missing [...] in state.json` | An earlier step did not finish; re-run the steps in order. |
| `AccessDeniedException` on invoke | Step 4: enable the model in Bedrock Model access. |
| Runtime `CREATE_FAILED` | `aws bedrock-agentcore-control get-agent-runtime --agent-runtime-id <id>` and read `failureReason`. Usually IAM delay; re-run Step 10. |
| Monitor log panel empty for prod | Prod writes to its own `...-prod` log group; use the latest `tools/monitor.py`. |
| `Failed to export span batch code: 400` in logs | Traces may not reach CloudWatch, so GenAI Observability and the evaluation judge scores stay blank. Check Step 7 first. |
| Actions cannot assume the role | Step 9 not done, wrong repo in the trust policy, or an `environment:` was added to the job (it changes the token subject). |
| Dev and prod share one memory | Do not use real user data on dev. |

More detail on every command: `docs/command-reference.md`.
