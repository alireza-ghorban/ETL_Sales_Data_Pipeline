import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

#Define the EMR cluster configuration
JOB_FLOW_OVERRIDES = {
    # Cluster details
    'Name': 'midterm-cluster',
    'ReleaseLabel': 'emr-6.10.0',
    'Instances': {
        # EC2 key pair for accessing instances
        'Ec2KeyName': 'midterm',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        # Define EC2 instances configuration for the cluster
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        # Specify subnet for instances
        'Ec2SubnetId': 'subnet-0719a6d2d02288bd2',
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefalutRole',
    'ServiceRole': 'EMR_DefaultRole',
    # Specify Spark and Hadoop applications for the cluster
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    # Configuration settings for Spark, Hive and Hive on Spark
    'Configurations': [
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        },
        {
            "Classification": "hive-site",
            "Properties": {
                "javax.jdo.option.ConnectionURL": "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true",
                "javax.jdo.option.ConnectionDriverName": "com.mysql.jdbc.Driver",
                "javax.jdo.option.ConnectionUserName": "hive",
                "javax.jdo.option.ConnectionPassword": "hive"
            }
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        }
    ],
    # S3 path for storing logs
    'LogUri': 's3://emr-logs-midterm-ag'
}


# Define Spark step to be executed on the cluster
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://midterm-artifacts-ag/transformations.py',
                "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"
            ]
        }
    }
]

# Default arguments for DAG
DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['alyresa@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Python function to retrieve S3 file details
def retrieve_s3_files(**kwargs):
    data= kwargs['dag_run'].conf 
    for key, value in data.items():
        kwargs['ti'].xcom_push(key=key, value=value)

# Define the DAG
dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

# Create PythonOperator task for retrieving S3 files
parse_request = PythonOperator(
    task_id = 'parse_request',
    provide_context = True,
    python_callable = retrieve_s3_files,
    dag = dag
) 

# Create tasks for EMR cluster creation, step addition, monitoring and termination
create_cluster = EmrCreateJobFlowOperator(
    task_id='create_cluster',
    aws_conn_id='aws_default',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default", 
    dag = dag
)

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# Define task dependencies
parse_request >> create_cluster >> step_adder >> step_checker >> terminate_cluster
