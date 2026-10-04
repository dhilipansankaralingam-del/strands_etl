"""Continuous (online) evaluation of live traffic with AgentCore Evaluations.

AgentCore samples sessions from the runtime's traces and scores them with the
built-in evaluators. Results appear in CloudWatch → GenAI Observability →
Bedrock AgentCore → your agent → "Evaluations" tab (and in the log group
/aws/bedrock-agentcore/evaluations/...).

python eval/setup_online_eval.py            # create / enable
python eval/setup_online_eval.py --disable  # pause it
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "deploy"))
from common import AGENT_NAME, PROJECT, control, load_state, require, save_state  # noqa: E402

EVALUATORS = ["Builtin.GoalSuccessRate", "Builtin.Helpfulness", "Builtin.Faithfulness",
              "Builtin.ToolSelectionAccuracy", "Builtin.Harmfulness"]

if __name__ == "__main__":
    s = load_state()
    require(s, "agent_runtime_id", "eval_role_arn")
    cp = control()

    if "--disable" in sys.argv:
        require(s, "online_eval_config_id")
        cp.update_online_evaluation_config(onlineEvaluationConfigId=s["online_eval_config_id"],
                                           executionStatus="DISABLED")
        sys.exit("Online evaluation disabled.")

    if s.get("online_eval_config_id"):
        print(f"Already configured: {s['online_eval_config_id']}")
        sys.exit(0)

    res = cp.create_online_evaluation_config(
        onlineEvaluationConfigName=f"{PROJECT.replace('-', '_')}_online_eval",
        description="Samples live multi-agent sessions and scores them with built-in evaluators",
        rule={
            "samplingConfig": {"samplingPercentage": 100.0},    # POC: evaluate everything
            "sessionConfig": {"sessionTimeoutMinutes": 15},     # session considered done after 15 idle min
        },
        dataSourceConfig={"cloudWatchLogs": {
            "logGroupNames": [f"/aws/bedrock-agentcore/runtimes/{s['agent_runtime_id']}-DEFAULT"],
            "serviceNames": [f"{AGENT_NAME}.DEFAULT"],
        }},
        evaluators=[{"evaluatorId": e} for e in EVALUATORS],
        evaluationExecutionRoleArn=s["eval_role_arn"],
        enableOnCreate=True,
    )
    cfg_id = res.get("onlineEvaluationConfigId")
    save_state(online_eval_config_id=cfg_id)
    print(f"Online evaluation created: {cfg_id} status={res.get('status')}")
