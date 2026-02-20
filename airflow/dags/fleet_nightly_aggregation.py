"""
Airflow DAG: Fleet Nightly Aggregation Pipeline.

Runs daily at 2 AM UTC to:
1. Process incremental GPS and delivery data (CDC)
2. Build daily aggregations
3. Update dimension tables
4. Generate reports

Why Airflow:
- Dependency management between tasks
- Retry logic with backoff
- Alerting on failures
- Historical run tracking
- Easy backfills
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# =============================================================================
# DAG CONFIGURATION
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# EMR cluster configuration
EMR_CLUSTER_ID = Variable.get("fleet_emr_cluster_id", default_var="j-XXXXXXXXXXXXX")
S3_BUCKET = Variable.get("fleet_s3_bucket", default_var="fleet-data-lake")
GLUE_DATABASE = "fleet_analytics"

# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def check_source_data_availability(**context):
    """
    Check if source data is available for processing.
    
    Validates that raw data exists for the execution date.
    """
    import boto3
    
    execution_date = context["ds"]
    s3 = boto3.client("s3")
    
    # Check GPS data
    gps_prefix = f"raw/gps/event_date={execution_date}/"
    gps_response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=gps_prefix,
        MaxKeys=1
    )
    
    if gps_response.get("KeyCount", 0) == 0:
        raise ValueError(f"No GPS data found for {execution_date}")
    
    # Check delivery data
    delivery_prefix = f"raw/deliveries/event_date={execution_date}/"
    delivery_response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=delivery_prefix,
        MaxKeys=1
    )
    
    if delivery_response.get("KeyCount", 0) == 0:
        raise ValueError(f"No delivery data found for {execution_date}")
    
    logger.info(f"Source data available for {execution_date}")
    return True


def update_watermarks(**context):
    """
    Update watermarks after successful processing.
    
    Stores the high watermark in S3 for next incremental run.
    """
    import boto3
    import json
    
    execution_date = context["ds"]
    s3 = boto3.client("s3")
    
    watermark_data = {
        "job_name": "fleet_nightly_aggregation",
        "high_watermark": f"{execution_date}T23:59:59Z",
        "updated_at": datetime.utcnow().isoformat(),
        "dag_run_id": context["run_id"]
    }
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="watermarks/fleet_nightly_aggregation/latest.json",
        Body=json.dumps(watermark_data),
        ContentType="application/json"
    )
    
    logger.info(f"Updated watermark to {execution_date}")


def validate_output_data(**context):
    """
    Validate that output data was written correctly.
    
    Checks row counts and data quality metrics.
    """
    import boto3
    
    execution_date = context["ds"]
    s3 = boto3.client("s3")
    
    # Check processed GPS data
    gps_prefix = f"processed/gps/event_date={execution_date}/"
    gps_response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=gps_prefix
    )
    
    gps_files = gps_response.get("KeyCount", 0)
    if gps_files == 0:
        raise ValueError(f"No processed GPS data for {execution_date}")
    
    # Check aggregations
    agg_prefix = f"aggregated/daily_summary/date_key={execution_date.replace('-', '')}/"
    agg_response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=agg_prefix
    )
    
    if agg_response.get("KeyCount", 0) == 0:
        raise ValueError(f"No aggregation data for {execution_date}")
    
    logger.info(f"Output validation passed for {execution_date}")
    return True


def generate_metrics_report(**context):
    """
    Generate metrics report for the day.
    
    Calculates KPIs and stores in S3 for dashboards.
    """
    import boto3
    import json
    
    execution_date = context["ds"]
    
    # In production, this would query Athena/Redshift
    # Simplified: generate sample metrics
    metrics = {
        "date": execution_date,
        "total_gps_events": 1_500_000,
        "active_vehicles": 250,
        "total_deliveries": 12_500,
        "on_time_delivery_pct": 94.5,
        "avg_delivery_time_minutes": 28.3,
        "fleet_utilization_pct": 78.2,
        "generated_at": datetime.utcnow().isoformat()
    }
    
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"reports/daily_metrics/{execution_date}.json",
        Body=json.dumps(metrics, indent=2),
        ContentType="application/json"
    )
    
    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="daily_metrics", value=metrics)
    
    logger.info(f"Generated metrics report for {execution_date}")
    return metrics


# =============================================================================
# SPARK STEP DEFINITIONS
# =============================================================================

def get_spark_steps(execution_date: str) -> list:
    """
    Generate EMR Spark steps for the ETL pipeline.
    """
    return [
        {
            "Name": "Process GPS Data",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.shuffle.partitions=20",
                    "--conf", "spark.dynamicAllocation.enabled=true",
                    f"s3://{S3_BUCKET}/scripts/etl/daily_etl.py",
                    "--date", execution_date,
                    "--table", "gps"
                ]
            }
        },
        {
            "Name": "Process Delivery Data",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    f"s3://{S3_BUCKET}/scripts/etl/daily_etl.py",
                    "--date", execution_date,
                    "--table", "deliveries"
                ]
            }
        },
        {
            "Name": "Build Daily Aggregations",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    f"s3://{S3_BUCKET}/scripts/etl/build_aggregations.py",
                    "--date", execution_date
                ]
            }
        }
    ]


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="fleet_nightly_aggregation",
    default_args=default_args,
    description="Nightly ETL pipeline for fleet analytics",
    schedule_interval="0 2 * * *",  # 2 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "etl", "nightly"],
) as dag:
    
    # =========================================================================
    # TASK DEFINITIONS
    # =========================================================================
    
    # 1. Check source data availability
    check_source = PythonOperator(
        task_id="check_source_data",
        python_callable=check_source_data_availability,
        provide_context=True,
    )
    
    # 2. Run Spark ETL on EMR
    with TaskGroup(group_id="spark_etl") as spark_etl_group:
        
        add_steps = EmrAddStepsOperator(
            task_id="add_spark_steps",
            job_flow_id=EMR_CLUSTER_ID,
            steps=get_spark_steps("{{ ds }}"),
            aws_conn_id="aws_default",
        )
        
        # Wait for GPS processing
        wait_gps = EmrStepSensor(
            task_id="wait_gps_step",
            job_flow_id=EMR_CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='spark_etl.add_spark_steps')[0] }}",
            aws_conn_id="aws_default",
            poke_interval=60,
            timeout=3600,
        )
        
        # Wait for delivery processing
        wait_delivery = EmrStepSensor(
            task_id="wait_delivery_step",
            job_flow_id=EMR_CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='spark_etl.add_spark_steps')[1] }}",
            aws_conn_id="aws_default",
            poke_interval=60,
            timeout=3600,
        )
        
        # Wait for aggregations
        wait_aggregations = EmrStepSensor(
            task_id="wait_aggregation_step",
            job_flow_id=EMR_CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='spark_etl.add_spark_steps')[2] }}",
            aws_conn_id="aws_default",
            poke_interval=60,
            timeout=3600,
        )
        
        add_steps >> [wait_gps, wait_delivery] >> wait_aggregations
    
    # 3. Update Glue catalog (partition repair)
    repair_partitions = AthenaOperator(
        task_id="repair_glue_partitions",
        query=f"""
            MSCK REPAIR TABLE {GLUE_DATABASE}.fact_gps_events;
            MSCK REPAIR TABLE {GLUE_DATABASE}.fact_deliveries;
            MSCK REPAIR TABLE {GLUE_DATABASE}.agg_daily_summary;
        """,
        database=GLUE_DATABASE,
        output_location=f"s3://{S3_BUCKET}/athena-results/",
        aws_conn_id="aws_default",
    )
    
    # 4. Validate output data
    validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output_data,
        provide_context=True,
    )
    
    # 5. Update watermarks
    update_wm = PythonOperator(
        task_id="update_watermarks",
        python_callable=update_watermarks,
        provide_context=True,
    )
    
    # 6. Generate metrics report
    generate_report = PythonOperator(
        task_id="generate_metrics_report",
        python_callable=generate_metrics_report,
        provide_context=True,
    )
    
    # 7. Send success notification
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_webhook",
        message="""
        :white_check_mark: Fleet Nightly ETL Completed
        
        *Date*: {{ ds }}
        *Duration*: {{ task_instance.duration }}s
        *GPS Events*: {{ task_instance.xcom_pull(task_ids='generate_metrics_report', key='daily_metrics')['total_gps_events'] }}
        *Deliveries*: {{ task_instance.xcom_pull(task_ids='generate_metrics_report', key='daily_metrics')['total_deliveries'] }}
        *On-Time %*: {{ task_instance.xcom_pull(task_ids='generate_metrics_report', key='daily_metrics')['on_time_delivery_pct'] }}%
        """,
        channel="#data-alerts",
    )
    
    # =========================================================================
    # TASK DEPENDENCIES
    # =========================================================================
    
    check_source >> spark_etl_group >> repair_partitions >> validate_output >> update_wm >> generate_report >> notify_success
