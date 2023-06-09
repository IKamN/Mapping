import pprint

def tmp_dag(docs,
            developer,
            database,
            id_is,
            topic,
            flows):
    res = f"""
\"""
#### Version 0.0.1
#### Docs: {docs}
#### Stream: Ozero dannykh
#### Team: LOADSUBO
#### Dev: {developer}
#### Sourcedb: \'streaming.smart_replication_change_request_{topic}_default\'
#### Targetdb: 1642_19: prod_repl_subo_{database}
\"""

import os
import json
from datetime import timedelta

from airflow import DAG
from airflow.models import DagRun
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
""" + '\n' \
          + \
          f"""DAG_ID = \"1642_19_datalake_{database}_{id_is}_load\"

stage_and_config_path = Variable.get("1642_19_CONFIG_STAGE", deserialize_json=True)
stage = stage_and_config_path["STAGE"]
config_path = stage_and_config_path["1642_19_CONFIG"]

with open(config_path) as cfg:
        config = json.load(cfg)

spark_etl_config = config.get("SPARK_ETL_CONF")
vault_conf = config.get("VAULT4SETL")

core_path = spark_etl_config["SPARK_ETL_JAR"]
spark_jars = spark_etl_config["DATALAKE_SPARK_JARS"]
etl_schema_kafka = spark_etl_config['ETL_SCHEMA']
keytab_dir = spark_etl_config["DATALAKE_KEYTAB_DIR"]
keytab_name = spark_etl_config["KEYTAB"]
principal = spark_etl_config["PRINCIPAL"]
keytab = os.path.join(keytab_dir, keytab_name)
truststore_path = spark_etl_config["subo_truststore_path"]
keystore_path = spark_etl_config["subo_keystore_path"]
logs_table = vault_conf["psqlStatusTable"]""" + '\n' \
          + \
          f"""
if stage == "d0":
        target_db = "test_etl_subo"
        etl_schema = "test_etl_subo"
elif stage == "if":
        target_db = "ift_repl_subo_{database}"
        etl_schema ="ift_etl_subo"
elif stage == "rr":
        target_db = "test_repl_subo_{database}"
        etl_schema = "test_etl_subo"
elif stage == "p0":
        target_db = "prod_repl_subo_{database}"
        etl_schema = "prod_etl_subo"
else:
        assert False, 'please, set "STAGE" variable'
""" \
+ \
"""
default_args = {
  "owner": "airflow", 
  "depends_on_past": False,
  "start_date": days_ago(1),
  "retries": 0
}
      
spark_conf = {
  "spark.master": "yarn",
  "spark.submit.deployMode": "cluster",
  "spark.driver.memory": "4g",
  "spark.executor.memory": "4g",
  "spark.executor.cores": "2",
  "spark.num.executors": "2",
  "spark.hadoop.hive.exec.dynamic.partition": "true",
  "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
  "spark.driver.userClassPathFirst": "true",
  "spark.executor.userClassPathFirst": "true",
}

common_info = {
  "targetSchema": etl_schema,
  "etlSchema": etl_schema_kafka,
  "logsTable": logs_table,
  "vault": vault_conf,
  """ \
+ \
f'"flagName": {id_is}' + '\n' \
"""
}
      
hive_common_info = {
  "targetSchema": target_db,
  "etlSchema": etl_schema,
  "logsTable": logs_table,
  "vault": vault_conf,
  """ \
+ \
f'"flagName": {id_is}' \
+ '\n' \
"}" \
+ '\n' \
"""
load_kafka_json = {
        "connection": "1642_19_datalake_subo_kafka_load_otpl",
        "commonInfo": common_info,
        "flows": [
            {
                "loadType": "Scd0Append",
                "source": {
                    """ + \
f"\"topic\": \"streaming.smart_replication_change_request_{topic}_default\"," + """
        \"failOnDataLoss\": \"false\",
        "incrementField": "hdp_processed_dttm",
    },
    "target": {""" + \
f"\"table\": \"streaming_smart_replication_change_request_{topic}_default\"" + """
            }
        }
    ]
}

scd_build_json = {
"connection": "1642_19_datalake_hive_load",
"commonInfo": hive_common_info, 
"flows":""" + f"""{pprint.pformat(flows, width=250, sort_dicts=False)}""" + '\n' \
"""} \n 

def check_previous_runs():
      dag_runs = DagRun.find(dag_id=DAG_ID)
      dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
      dag_runs = list(filter(lambda x: x.state == "running", dag_runs[1:]))
      if dag_runs:
              raise AirflowSkipException("One of previous job is not finished")


def spark_operator(task_id: str, name: str, json_load: dict):
      return SparkSubmitOperator(
              task_id=task_id,
              name=name,
              files=f'{keystore_path},{truststore_path}',
              application_args=[json.dumps(json_load).replace("{{", "{ {").replace("}}", "} }")],
              application=core_path,
              java_class='sparketl.Main',
              keytab=keytab,
              principal=principal,
              jars=spark_jars,
              conf=spark_conf,
              verbose=False,
              dag=dag
      )


with DAG(
  dag_id=DAG_ID,
  default_args=default_args,
  tags=['SparkETL', 'VaultSETL', 'Subo'],
  schedule_interval='0 */3 * * *',
  catchup=False,
  max_active_runs=1
) as dag:
      load_kafka = spark_operator("load_kafka", f'{DAG_ID}.load_kafka', load_kafka_json)
      load_status = PythonOperator(task_id="check_previous_runs", python_callable=check_previous_runs)
      scd_build = spark_operator("scd_build", f'{DAG_ID}.scd_build', scd_build_json)

      load_kafka >> load_status >> scd_build"""

    return res
