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
