from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pendulum
import logging

local_tz = pendulum.timezone("Asia/Kolkata")

args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2025, 6, 3, tz=local_tz)
}

def extract_metadata_from_pg(**context):
    """Extract metadata for all tables"""

    # Initialize PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')

    # Get table metadata
    metadata_query = """
        select
            parm_name,
            parm_value
        from meta_schema.airflow_dags_metadata
        where batch_name ='SelfFinanceReportsBatch'
        and sub_task ='Loading'
        and parm_name ='file_to_process'
    """

    # Execute query and get results
    records = postgres_hook.get_records(metadata_query)

    # Convert to dictionary format
    metadata = {}
    for record in records:
        parm_name = record[0]
        parm_value = record[1]
        metadata[parm_name] = parm_value

    # Log metadata
    logging.info(f"Extracted metadata for {parm_name} : {parm_value}")

    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='file_to_process', value=parm_value)

    return metadata

with DAG(dag_id='self-finance-reports', schedule_interval='0 22 * * *', default_args=args, catchup=False, tags=['dev']) as dag:

    mail_data_extraction_into_json_file = SimpleHttpOperator(
        task_id='extract-mails-into-json-file-jy-nb',
        http_conn_id='python_service',  # Defined in Airflow Connections UI
        endpoint='extract-mails-into-json-file-jy-nb',
        method='POST',
        response_check=lambda response: response.status_code == 200 and response.json()['status'] == 'success',
        log_response=True,
    )

# Note: Below volume is shared between python container, host machine and ftp_server
# python container: /home/commonid/code/ext_code/fetch_mail_using_imap_lib/data_files/ftpuser
# host machine: /home/nileshp/learning/python/code/ext_code/fetch_mail_using_imap_lib/data_files
# ftp_server: /home/vsftpd/ftpuser/

#So as soon as mail_data_extraction_into_json_file SU the json file will be uploaded/available on ftp_server automatically
# then josn file from ftp_server will be downloaded by datanode by task dowload_json_file_to_datanode

    extract_metadata = PythonOperator(
        task_id='extract_metadata_from_pg',
        python_callable=extract_metadata_from_pg
    )

    copy_json_file_to_datanode = SSHOperator(
        task_id='dowload_json_file_to_datanode',
        ssh_conn_id='hadoop_datanode',   # Make sure this connection exists in UI
        command="""
        FILE_NAME="{{ task_instance.xcom_pull(task_ids='extract_metadata_from_pg', key='file_to_process') }}"
        curl -u ftpuser:mypassword ftp://ftp_server/ftpuser/$FILE_NAME -o /hadoop-data/$FILE_NAME
        """
    )

    copy_json_file_local_to_hdfs = SSHOperator(
        task_id='copy_json_file_to_hdfs_from_datanode_local',
        ssh_conn_id='hadoop_datanode',   # Make sure this connection exists in UI
        command="""
        export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
        FILE_NAME="{{ task_instance.xcom_pull(task_ids='extract_metadata_from_pg', key='file_to_process') }}"
        /opt/hadoop-3.2.1/bin//hadoop fs -copyFromLocal -f /hadoop-data/$FILE_NAME /user/data/json_in_unrecommended_format
        """
    )

    mail_data_load_into_hive = SimpleHttpOperator(
        task_id='trasform-and-load-mails-from-json-file-to-hive-jy-nb',
        http_conn_id='spark_service',  # Defined in Airflow Connections UI
        endpoint='trasform-and-load-mails-from-json-file-to-hive-jy-nb',
        method='POST',
        response_check=lambda response: response.status_code == 200 and response.json()['status'] == 'success',
        log_response=True,
    )

    update_current_watermark_in_metadata_table = SimpleHttpOperator(
        task_id='update-current-watermark-in-metadata-table-jy-nb',
        http_conn_id='spark_service',  # Defined in Airflow Connections UI
        endpoint='update-current-watermark-in-metadata-table-jy-nb',
        method='POST',
        response_check=lambda response: response.status_code == 200 and response.json()['status'] == 'success',
        log_response=True,
    )

mail_data_extraction_into_json_file >> extract_metadata >> copy_json_file_to_datanode >> copy_json_file_local_to_hdfs >> mail_data_load_into_hive >> update_current_watermark_in_metadata_table
