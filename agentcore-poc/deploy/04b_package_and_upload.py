"""Step 6b (no-Docker alternative) - package the agent as a zip and upload it to S3.

AgentCore Runtime can run a Python zip directly instead of a container image, which
means no Docker, no ECR and no arm64 emulation - everything works in AWS CloudShell.

What goes in the zip:
    main.py              entry point (starts the same BedrockAgentCoreApp)
    agent/               our package
    <dependencies>       installed at the zip root as linux/arm64 wheels

The wheels are *downloaded* for arm64, not compiled, so this runs fine on an x86
machine or in CloudShell.

    python deploy/04b_package_and_upload.py            # build + upload
    python deploy/04b_package_and_upload.py --local    # build only, no AWS calls

Limits enforced by the service: 250 MB zipped, 750 MB unzipped.
"""
import argparse
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from common import AGENT_NAME, REGION, account_id, save_state

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
PKG = BUILD / "package"
ZIP = BUILD / "deployment_package.zip"
PY_VERSION = "3.13"          # matches runtime enum PYTHON_3_13
RUNTIME_ENUM = "PYTHON_3_13"

MAIN_SHIM = '''"""AgentCore direct-code entry point. Starts the same app as the container build."""
from agent.main import app

if __name__ == "__main__":
    app.run()
'''


def install_dependencies() -> None:
    """Install linux/arm64 wheels into the package directory."""
    req = str(ROOT / "requirements.txt")
    uv = shutil.which("uv")
    if uv:
        cmd = [uv, "pip", "install", "--python-platform", "aarch64-manylinux2014",
               "--python-version", PY_VERSION, "--target", str(PKG), "--only-binary=:all:", "-r", req]
    else:
        cmd = [sys.executable, "-m", "pip", "install", "--quiet", "--target", str(PKG),
               "--only-binary=:all:", "--platform", "manylinux2014_aarch64",
               "--python-version", PY_VERSION, "-r", req]
    print(f"==> Installing arm64 dependencies with {'uv' if uv else 'pip'}")
    subprocess.run(cmd, check=True)


def build_zip() -> Path:
    if BUILD.exists():
        shutil.rmtree(BUILD)
    PKG.mkdir(parents=True)
    install_dependencies()

    print("==> Adding agent code")
    shutil.copytree(ROOT / "agent", PKG / "agent",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    (PKG / "main.py").write_text(MAIN_SHIM)

    for cache in PKG.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)

    print("==> Zipping (files 644, dirs 755 as the service requires)")
    with zipfile.ZipFile(ZIP, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for path in sorted(PKG.rglob("*")):
            rel = str(path.relative_to(PKG))
            if path.is_dir():
                info = zipfile.ZipInfo(rel + "/")
                info.external_attr = (0o755 << 16) | 0x10
                z.writestr(info, b"")
            else:
                info = zipfile.ZipInfo(rel)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = ((0o755 if os.access(path, os.X_OK) else 0o644) << 16)
                z.writestr(info, path.read_bytes())

    zipped = ZIP.stat().st_size / 1e6
    unzipped = sum(f.stat().st_size for f in PKG.rglob("*") if f.is_file()) / 1e6
    print(f"    {ZIP.name}: {zipped:.1f} MB zipped / {unzipped:.1f} MB unzipped")
    if zipped > 250 or unzipped > 750:
        sys.exit("Package exceeds the AgentCore limits (250 MB zipped / 750 MB unzipped).")
    return ZIP


def ensure_bucket(name: str, acct: str) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.head_bucket(Bucket=name, ExpectedBucketOwner=acct)
        print(f"==> Using existing bucket {name}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket", "403"):
            raise
    print(f"==> Creating bucket {name}")
    kwargs = {"Bucket": name}
    if REGION != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": REGION}
    s3.create_bucket(**kwargs)
    s3.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True,
        "BlockPublicPolicy": True, "RestrictPublicBuckets": True})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="build the zip but do not upload")
    args = ap.parse_args()

    path = build_zip()
    if args.local:
        sys.exit(f"Built {path} (not uploaded).")

    acct = account_id()
    bucket = f"bedrock-agentcore-code-{acct}-{REGION}"
    key = f"{AGENT_NAME}/deployment_package.zip"
    ensure_bucket(bucket, acct)

    print(f"==> Uploading s3://{bucket}/{key}")
    boto3.client("s3", region_name=REGION).upload_file(
        str(path), bucket, key, ExtraArgs={"ExpectedBucketOwner": acct})

    save_state(code_bucket=bucket, code_key=key, code_runtime=RUNTIME_ENUM)
    print("Saved code_bucket / code_key to deploy/state.json")
    print("Next: python deploy/05_deploy_runtime.py")
