"""
Create IAM roles required for EMR.
"""

import boto3
import json
import time

iam = boto3.client('iam')


def create_emr_service_role():
    """Create EMR service role."""
    role_name = 'EMR_DefaultRole'
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "elasticmapreduce.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='EMR Service Role'
        )
        print(f"Created role: {role_name}")
        
        # Attach policy
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
        )
        print(f"Attached AmazonElasticMapReduceRole policy")
        
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"Role {role_name} already exists")


def create_emr_ec2_role():
    """Create EMR EC2 instance profile role."""
    role_name = 'EMR_EC2_DefaultRole'
    profile_name = 'EMR_EC2_DefaultRole'
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='EMR EC2 Instance Role'
        )
        print(f"Created role: {role_name}")
        
        # Attach policies
        policies = [
            'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role',
            'arn:aws:iam::aws:policy/AmazonS3FullAccess',
            'arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess'
        ]
        
        for policy_arn in policies:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            print(f"Attached policy: {policy_arn.split('/')[-1]}")
        
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"Role {role_name} already exists")
    
    # Create instance profile
    try:
        iam.create_instance_profile(InstanceProfileName=profile_name)
        print(f"Created instance profile: {profile_name}")
        
        iam.add_role_to_instance_profile(
            InstanceProfileName=profile_name,
            RoleName=role_name
        )
        print(f"Added role to instance profile")
        
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"Instance profile {profile_name} already exists")


def main():
    print("Setting up EMR IAM roles...")
    create_emr_service_role()
    create_emr_ec2_role()
    
    print("\nWaiting 10 seconds for IAM propagation...")
    time.sleep(10)
    
    print("\nEMR roles setup complete!")


if __name__ == "__main__":
    main()
