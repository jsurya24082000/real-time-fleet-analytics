"""
EMR Job Submission Script for Fleet Analytics.

Submits Spark jobs to an EMR cluster for:
- Daily ETL processing
- Incremental CDC processing
- Ad-hoc analytics

Why EMR:
- Managed Spark/Hadoop cluster
- Auto-scaling based on workload
- Cost-effective with spot instances
- Integration with S3, Glue, Redshift
"""

import boto3
import argparse
import time
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# EMR CONFIGURATION
# =============================================================================

EMR_CONFIG = {
    "cluster_name": "fleet-analytics-cluster",
    "log_uri": "s3://fleet-data-lake/emr-logs/",
    "release_label": "emr-6.15.0",
    "applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "Presto"}
    ],
    "instance_config": {
        "master": {
            "instance_type": "m5.xlarge",
            "instance_count": 1,
            "market": "ON_DEMAND"
        },
        "core": {
            "instance_type": "m5.xlarge",
            "instance_count": 2,
            "market": "SPOT",
            "bid_price": "0.10"
        },
        "task": {
            "instance_type": "m5.xlarge",
            "instance_count": 0,  # Auto-scaling
            "market": "SPOT",
            "bid_price": "0.10"
        }
    },
    "auto_scaling": {
        "min_capacity": 2,
        "max_capacity": 10,
        "scale_out_cooldown": 300,
        "scale_in_cooldown": 300
    }
}

SPARK_CONFIG = {
    "spark.sql.shuffle.partitions": "20",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "10",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}


# =============================================================================
# EMR CLIENT
# =============================================================================

class EMRClient:
    """Client for interacting with AWS EMR."""
    
    def __init__(self, region: str = "us-east-1"):
        self.emr = boto3.client("emr", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.region = region
    
    def create_cluster(
        self,
        name: str = None,
        keep_alive: bool = True,
        auto_terminate: bool = False
    ) -> str:
        """
        Create a new EMR cluster.
        
        Args:
            name: Cluster name
            keep_alive: Keep cluster running after steps complete
            auto_terminate: Terminate after all steps complete
            
        Returns:
            Cluster ID
        """
        name = name or f"{EMR_CONFIG['cluster_name']}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        instance_groups = [
            {
                "Name": "Master",
                "InstanceRole": "MASTER",
                "InstanceType": EMR_CONFIG["instance_config"]["master"]["instance_type"],
                "InstanceCount": EMR_CONFIG["instance_config"]["master"]["instance_count"],
                "Market": EMR_CONFIG["instance_config"]["master"]["market"]
            },
            {
                "Name": "Core",
                "InstanceRole": "CORE",
                "InstanceType": EMR_CONFIG["instance_config"]["core"]["instance_type"],
                "InstanceCount": EMR_CONFIG["instance_config"]["core"]["instance_count"],
                "Market": EMR_CONFIG["instance_config"]["core"]["market"],
                "BidPrice": EMR_CONFIG["instance_config"]["core"]["bid_price"]
            }
        ]
        
        response = self.emr.run_job_flow(
            Name=name,
            LogUri=EMR_CONFIG["log_uri"],
            ReleaseLabel=EMR_CONFIG["release_label"],
            Applications=EMR_CONFIG["applications"],
            Instances={
                "InstanceGroups": instance_groups,
                "Ec2KeyName": "fleet-analytics-key",
                "KeepJobFlowAliveWhenNoSteps": keep_alive,
                "TerminationProtected": False,
                "Ec2SubnetId": "subnet-xxxxxxxx"  # Replace with actual subnet
            },
            Configurations=[
                {
                    "Classification": "spark-defaults",
                    "Properties": SPARK_CONFIG
                },
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": 
                            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                }
            ],
            ServiceRole="EMR_DefaultRole",
            JobFlowRole="EMR_EC2_DefaultRole",
            VisibleToAllUsers=True,
            Tags=[
                {"Key": "Project", "Value": "fleet-analytics"},
                {"Key": "Environment", "Value": "production"}
            ]
        )
        
        cluster_id = response["JobFlowId"]
        logger.info(f"Created EMR cluster: {cluster_id}")
        
        return cluster_id
    
    def get_cluster_status(self, cluster_id: str) -> Dict[str, Any]:
        """Get cluster status."""
        response = self.emr.describe_cluster(ClusterId=cluster_id)
        cluster = response["Cluster"]
        
        return {
            "id": cluster["Id"],
            "name": cluster["Name"],
            "state": cluster["Status"]["State"],
            "state_change_reason": cluster["Status"].get("StateChangeReason", {}),
            "master_public_dns": cluster.get("MasterPublicDnsName"),
            "normalized_instance_hours": cluster.get("NormalizedInstanceHours", 0)
        }
    
    def wait_for_cluster(self, cluster_id: str, timeout: int = 1800) -> bool:
        """
        Wait for cluster to be ready.
        
        Args:
            cluster_id: EMR cluster ID
            timeout: Maximum wait time in seconds
            
        Returns:
            True if cluster is ready, False if failed/timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_cluster_status(cluster_id)
            state = status["state"]
            
            logger.info(f"Cluster {cluster_id} state: {state}")
            
            if state == "WAITING":
                logger.info(f"Cluster {cluster_id} is ready")
                return True
            elif state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
                logger.error(f"Cluster {cluster_id} failed: {status['state_change_reason']}")
                return False
            
            time.sleep(30)
        
        logger.error(f"Timeout waiting for cluster {cluster_id}")
        return False
    
    def submit_spark_step(
        self,
        cluster_id: str,
        script_path: str,
        step_name: str,
        args: List[str] = None,
        action_on_failure: str = "CONTINUE"
    ) -> str:
        """
        Submit a Spark step to EMR cluster.
        
        Args:
            cluster_id: EMR cluster ID
            script_path: S3 path to Spark script
            step_name: Name for the step
            args: Additional arguments for the script
            action_on_failure: CONTINUE, CANCEL_AND_WAIT, or TERMINATE_CLUSTER
            
        Returns:
            Step ID
        """
        spark_args = [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--master", "yarn",
            "--conf", f"spark.sql.shuffle.partitions={SPARK_CONFIG['spark.sql.shuffle.partitions']}",
            "--conf", "spark.dynamicAllocation.enabled=true",
            script_path
        ]
        
        if args:
            spark_args.extend(args)
        
        step = {
            "Name": step_name,
            "ActionOnFailure": action_on_failure,
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": spark_args
            }
        }
        
        response = self.emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )
        
        step_id = response["StepIds"][0]
        logger.info(f"Submitted step {step_name}: {step_id}")
        
        return step_id
    
    def get_step_status(self, cluster_id: str, step_id: str) -> Dict[str, Any]:
        """Get step status."""
        response = self.emr.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        step = response["Step"]
        
        return {
            "id": step["Id"],
            "name": step["Name"],
            "state": step["Status"]["State"],
            "state_change_reason": step["Status"].get("StateChangeReason", {})
        }
    
    def wait_for_step(self, cluster_id: str, step_id: str, timeout: int = 3600) -> bool:
        """
        Wait for step to complete.
        
        Args:
            cluster_id: EMR cluster ID
            step_id: Step ID
            timeout: Maximum wait time in seconds
            
        Returns:
            True if step completed successfully
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_step_status(cluster_id, step_id)
            state = status["state"]
            
            logger.info(f"Step {step_id} state: {state}")
            
            if state == "COMPLETED":
                logger.info(f"Step {step_id} completed successfully")
                return True
            elif state in ["FAILED", "CANCELLED"]:
                logger.error(f"Step {step_id} failed: {status['state_change_reason']}")
                return False
            
            time.sleep(30)
        
        logger.error(f"Timeout waiting for step {step_id}")
        return False
    
    def terminate_cluster(self, cluster_id: str) -> None:
        """Terminate EMR cluster."""
        self.emr.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info(f"Terminated cluster {cluster_id}")


# =============================================================================
# JOB DEFINITIONS
# =============================================================================

class FleetETLJobs:
    """Fleet analytics ETL job definitions."""
    
    def __init__(self, emr_client: EMRClient, s3_bucket: str = "fleet-data-lake"):
        self.emr = emr_client
        self.s3_bucket = s3_bucket
        self.scripts_path = f"s3://{s3_bucket}/scripts/etl"
    
    def run_daily_etl(self, cluster_id: str, process_date: str) -> Dict[str, Any]:
        """
        Run daily ETL job.
        
        Args:
            cluster_id: EMR cluster ID
            process_date: Date to process (YYYY-MM-DD)
            
        Returns:
            Job result with metrics
        """
        logger.info(f"Running daily ETL for {process_date}")
        
        # Submit GPS processing step
        gps_step_id = self.emr.submit_spark_step(
            cluster_id=cluster_id,
            script_path=f"{self.scripts_path}/daily_etl.py",
            step_name=f"GPS ETL - {process_date}",
            args=["--date", process_date, "--table", "gps"]
        )
        
        # Wait for GPS step
        gps_success = self.emr.wait_for_step(cluster_id, gps_step_id)
        
        # Submit delivery processing step
        delivery_step_id = self.emr.submit_spark_step(
            cluster_id=cluster_id,
            script_path=f"{self.scripts_path}/daily_etl.py",
            step_name=f"Delivery ETL - {process_date}",
            args=["--date", process_date, "--table", "deliveries"]
        )
        
        # Wait for delivery step
        delivery_success = self.emr.wait_for_step(cluster_id, delivery_step_id)
        
        # Submit aggregation step
        agg_step_id = self.emr.submit_spark_step(
            cluster_id=cluster_id,
            script_path=f"{self.scripts_path}/build_aggregations.py",
            step_name=f"Aggregations - {process_date}",
            args=["--date", process_date]
        )
        
        # Wait for aggregation step
        agg_success = self.emr.wait_for_step(cluster_id, agg_step_id)
        
        return {
            "process_date": process_date,
            "gps_step": {"id": gps_step_id, "success": gps_success},
            "delivery_step": {"id": delivery_step_id, "success": delivery_success},
            "aggregation_step": {"id": agg_step_id, "success": agg_success},
            "overall_success": gps_success and delivery_success and agg_success
        }
    
    def run_incremental_cdc(self, cluster_id: str) -> Dict[str, Any]:
        """
        Run incremental CDC job.
        
        Processes only new/changed data since last run.
        """
        logger.info("Running incremental CDC job")
        
        step_id = self.emr.submit_spark_step(
            cluster_id=cluster_id,
            script_path=f"{self.scripts_path}/cdc_processor.py",
            step_name=f"Incremental CDC - {datetime.now().strftime('%Y%m%d-%H%M%S')}"
        )
        
        success = self.emr.wait_for_step(cluster_id, step_id)
        
        return {
            "step_id": step_id,
            "success": success
        }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Fleet Analytics EMR Job Submission")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Create cluster command
    create_parser = subparsers.add_parser("create-cluster", help="Create EMR cluster")
    create_parser.add_argument("--name", type=str, help="Cluster name")
    create_parser.add_argument("--wait", action="store_true", help="Wait for cluster to be ready")
    
    # Submit job command
    submit_parser = subparsers.add_parser("submit-job", help="Submit Spark job")
    submit_parser.add_argument("--cluster-id", type=str, required=True, help="EMR cluster ID")
    submit_parser.add_argument("--job", type=str, required=True, 
                               choices=["daily-etl", "incremental-cdc"],
                               help="Job to run")
    submit_parser.add_argument("--date", type=str, help="Process date (YYYY-MM-DD)")
    
    # Get status command
    status_parser = subparsers.add_parser("status", help="Get cluster/step status")
    status_parser.add_argument("--cluster-id", type=str, required=True, help="EMR cluster ID")
    status_parser.add_argument("--step-id", type=str, help="Step ID (optional)")
    
    # Terminate command
    terminate_parser = subparsers.add_parser("terminate", help="Terminate cluster")
    terminate_parser.add_argument("--cluster-id", type=str, required=True, help="EMR cluster ID")
    
    args = parser.parse_args()
    
    emr_client = EMRClient()
    
    if args.command == "create-cluster":
        cluster_id = emr_client.create_cluster(name=args.name)
        print(f"Cluster ID: {cluster_id}")
        
        if args.wait:
            emr_client.wait_for_cluster(cluster_id)
    
    elif args.command == "submit-job":
        jobs = FleetETLJobs(emr_client)
        
        if args.job == "daily-etl":
            process_date = args.date or datetime.now().strftime("%Y-%m-%d")
            result = jobs.run_daily_etl(args.cluster_id, process_date)
        elif args.job == "incremental-cdc":
            result = jobs.run_incremental_cdc(args.cluster_id)
        
        print(json.dumps(result, indent=2))
    
    elif args.command == "status":
        if args.step_id:
            status = emr_client.get_step_status(args.cluster_id, args.step_id)
        else:
            status = emr_client.get_cluster_status(args.cluster_id)
        
        print(json.dumps(status, indent=2))
    
    elif args.command == "terminate":
        emr_client.terminate_cluster(args.cluster_id)
        print(f"Terminated cluster {args.cluster_id}")


if __name__ == "__main__":
    main()
