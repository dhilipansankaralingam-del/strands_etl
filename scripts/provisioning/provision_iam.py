#!/usr/bin/env python3
"""
IAM Provisioning Script for ETL Framework
==========================================

Creates all required IAM policies, roles, and instance profiles for:
- AWS Glue ETL jobs
- Amazon EMR clusters
- Amazon EKS clusters
- Amazon ECS tasks
- Lambda notifications (Slack/Teams)
- DynamoDB audit tables
- S3 data lake access

Usage:
    python provision_iam.py --region us-east-1 --account-id 123456789012
    python provision_iam.py --region us-east-1 --account-id 123456789012 --dry-run
    python provision_iam.py --region us-east-1 --account-id 123456789012 --delete

Prerequisites:
    - AWS CLI configured with admin permissions
    - boto3 installed: pip install boto3
"""

import boto3
import json
import argparse
import os
import sys
from pathlib import Path
from botocore.exceptions import ClientError


class IAMProvisioner:
    """Provisions IAM resources for ETL Framework."""

    def __init__(self, region: str, account_id: str, dry_run: bool = False):
        self.region = region
        self.account_id = account_id
        self.dry_run = dry_run
        self.iam = boto3.client('iam', region_name=region)

        # Get the base path for IAM files
        self.base_path = Path(__file__).parent.parent.parent / 'iam'

        # Track created resources
        self.created_policies = []
        self.created_roles = []
        self.created_instance_profiles = []

    def load_json_file(self, filepath: str) -> dict:
        """Load and parse a JSON file."""
        full_path = self.base_path / filepath
        with open(full_path, 'r') as f:
            return json.load(f)

    def create_policy(self, policy_name: str, policy_file: str) -> str:
        """Create an IAM policy from a JSON file."""
        policy_document = self.load_json_file(f'policies/{policy_file}')
        policy_arn = f'arn:aws:iam::{self.account_id}:policy/{policy_name}'

        print(f"Creating policy: {policy_name}")

        if self.dry_run:
            print(f"  [DRY RUN] Would create policy: {policy_arn}")
            return policy_arn

        try:
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=f'ETL Framework policy for {policy_name}',
                Tags=[
                    {'Key': 'Project', 'Value': 'ETL-Framework'},
                    {'Key': 'ManagedBy', 'Value': 'IAM-Provisioning'}
                ]
            )
            policy_arn = response['Policy']['Arn']
            self.created_policies.append(policy_arn)
            print(f"  Created: {policy_arn}")
            return policy_arn
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"  Policy already exists: {policy_arn}")
                return policy_arn
            raise

    def create_role(self, role_config_file: str) -> str:
        """Create an IAM role with attached policies."""
        config = self.load_json_file(f'roles/{role_config_file}')
        role_name = config['RoleName']

        print(f"\nCreating role: {role_name}")

        # Load trust policy
        trust_policy = self.load_json_file(f'trust_policies/{config["TrustPolicy"]}')

        if self.dry_run:
            print(f"  [DRY RUN] Would create role: {role_name}")
            print(f"  [DRY RUN] Trust policy: {config['TrustPolicy']}")
            print(f"  [DRY RUN] Managed policies: {config.get('ManagedPolicies', [])}")
            print(f"  [DRY RUN] Custom policies: {config.get('CustomPolicies', [])}")
            return f'arn:aws:iam::{self.account_id}:role/{role_name}'

        try:
            # Create the role
            response = self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=config.get('Description', f'ETL Framework role: {role_name}'),
                Tags=[
                    {'Key': k, 'Value': v}
                    for k, v in config.get('Tags', {}).items()
                ]
            )
            role_arn = response['Role']['Arn']
            self.created_roles.append(role_name)
            print(f"  Created role: {role_arn}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"  Role already exists: {role_name}")
                role_arn = f'arn:aws:iam::{self.account_id}:role/{role_name}'
            else:
                raise

        # Attach managed policies
        for managed_policy_arn in config.get('ManagedPolicies', []):
            try:
                self.iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=managed_policy_arn
                )
                print(f"  Attached managed policy: {managed_policy_arn}")
            except ClientError as e:
                if 'already attached' not in str(e).lower():
                    print(f"  Warning: Could not attach {managed_policy_arn}: {e}")

        # Create and attach custom policies
        for custom_policy_file in config.get('CustomPolicies', []):
            policy_name = f'etl-{custom_policy_file.replace(".json", "").replace("_", "-")}'
            policy_arn = self.create_policy(policy_name, custom_policy_file)
            try:
                self.iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                print(f"  Attached custom policy: {policy_arn}")
            except ClientError as e:
                if 'already attached' not in str(e).lower():
                    print(f"  Warning: Could not attach {policy_arn}: {e}")

        # Create instance profile if specified
        if 'InstanceProfile' in config:
            self.create_instance_profile(config['InstanceProfile'], role_name)

        return role_arn

    def create_instance_profile(self, profile_name: str, role_name: str):
        """Create an instance profile and attach a role to it."""
        print(f"  Creating instance profile: {profile_name}")

        if self.dry_run:
            print(f"  [DRY RUN] Would create instance profile: {profile_name}")
            return

        try:
            self.iam.create_instance_profile(
                InstanceProfileName=profile_name,
                Tags=[
                    {'Key': 'Project', 'Value': 'ETL-Framework'},
                    {'Key': 'ManagedBy', 'Value': 'IAM-Provisioning'}
                ]
            )
            self.created_instance_profiles.append(profile_name)
            print(f"    Created instance profile: {profile_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"    Instance profile already exists: {profile_name}")
            else:
                raise

        try:
            self.iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name
            )
            print(f"    Added role {role_name} to instance profile")
        except ClientError as e:
            if 'already added' not in str(e).lower() and 'LimitExceeded' not in str(e):
                print(f"    Warning: Could not add role to profile: {e}")

    def provision_all(self):
        """Provision all IAM resources."""
        print("=" * 60)
        print("ETL Framework IAM Provisioning")
        print("=" * 60)
        print(f"Region: {self.region}")
        print(f"Account ID: {self.account_id}")
        print(f"Dry Run: {self.dry_run}")
        print("=" * 60)

        # List of role configuration files
        role_configs = [
            'etl_glue_role.json',
            'etl_emr_role.json',
            'etl_emr_ec2_role.json',
            'etl_eks_role.json',
            'etl_eks_node_role.json',
            'etl_ecs_task_role.json',
            'etl_lambda_notification_role.json'
        ]

        for role_config in role_configs:
            try:
                self.create_role(role_config)
            except Exception as e:
                print(f"  ERROR creating role from {role_config}: {e}")

        print("\n" + "=" * 60)
        print("Provisioning Summary")
        print("=" * 60)
        print(f"Policies created: {len(self.created_policies)}")
        print(f"Roles created: {len(self.created_roles)}")
        print(f"Instance profiles created: {len(self.created_instance_profiles)}")

        if self.dry_run:
            print("\n[DRY RUN] No resources were actually created.")

    def delete_all(self):
        """Delete all ETL Framework IAM resources."""
        print("=" * 60)
        print("ETL Framework IAM Cleanup")
        print("=" * 60)
        print(f"Region: {self.region}")
        print(f"Account ID: {self.account_id}")
        print(f"Dry Run: {self.dry_run}")
        print("=" * 60)

        # List of resources to delete
        role_names = [
            'etl-glue-execution-role',
            'etl-emr-execution-role',
            'etl-emr-ec2-role',
            'etl-eks-cluster-role',
            'etl-eks-node-role',
            'etl-ecs-task-execution-role',
            'etl-lambda-notification-role'
        ]

        instance_profiles = [
            'etl-emr-ec2-instance-profile',
            'etl-eks-node-instance-profile'
        ]

        for profile_name in instance_profiles:
            try:
                if self.dry_run:
                    print(f"[DRY RUN] Would delete instance profile: {profile_name}")
                    continue

                # Remove roles from instance profile
                try:
                    response = self.iam.get_instance_profile(InstanceProfileName=profile_name)
                    for role in response['InstanceProfile']['Roles']:
                        self.iam.remove_role_from_instance_profile(
                            InstanceProfileName=profile_name,
                            RoleName=role['RoleName']
                        )
                except ClientError:
                    pass

                self.iam.delete_instance_profile(InstanceProfileName=profile_name)
                print(f"Deleted instance profile: {profile_name}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchEntity':
                    print(f"Warning: Could not delete {profile_name}: {e}")

        for role_name in role_names:
            try:
                if self.dry_run:
                    print(f"[DRY RUN] Would delete role: {role_name}")
                    continue

                # Detach all policies
                try:
                    policies = self.iam.list_attached_role_policies(RoleName=role_name)
                    for policy in policies['AttachedPolicies']:
                        self.iam.detach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['PolicyArn']
                        )
                except ClientError:
                    pass

                self.iam.delete_role(RoleName=role_name)
                print(f"Deleted role: {role_name}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchEntity':
                    print(f"Warning: Could not delete {role_name}: {e}")

        # Delete custom policies
        try:
            paginator = self.iam.get_paginator('list_policies')
            for page in paginator.paginate(Scope='Local'):
                for policy in page['Policies']:
                    if policy['PolicyName'].startswith('etl-'):
                        if self.dry_run:
                            print(f"[DRY RUN] Would delete policy: {policy['PolicyName']}")
                            continue

                        # Delete all policy versions except default
                        versions = self.iam.list_policy_versions(PolicyArn=policy['Arn'])
                        for version in versions['Versions']:
                            if not version['IsDefaultVersion']:
                                self.iam.delete_policy_version(
                                    PolicyArn=policy['Arn'],
                                    VersionId=version['VersionId']
                                )

                        self.iam.delete_policy(PolicyArn=policy['Arn'])
                        print(f"Deleted policy: {policy['PolicyName']}")
        except ClientError as e:
            print(f"Warning: Could not list/delete policies: {e}")


def main():
    parser = argparse.ArgumentParser(description='Provision IAM resources for ETL Framework')
    parser.add_argument('--region', required=True, help='AWS region')
    parser.add_argument('--account-id', required=True, help='AWS account ID')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--delete', action='store_true', help='Delete all ETL Framework IAM resources')

    args = parser.parse_args()

    provisioner = IAMProvisioner(
        region=args.region,
        account_id=args.account_id,
        dry_run=args.dry_run
    )

    if args.delete:
        provisioner.delete_all()
    else:
        provisioner.provision_all()


if __name__ == '__main__':
    main()
