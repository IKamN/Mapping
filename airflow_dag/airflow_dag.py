import airflow_dag.dag_template.template as dt
import typing

if typing.TYPE_CHECKING:
    from app.app import App

def save_airflow_dag(app: "App"):
    """
    :param params: parameters from config
    :param flows: flows with paths for dag
    :return: create dag file in directory
    """
    import os
    file_path: str = os.path.join(f"{app.config.file_dir}", f"1642_19_datalake_{app.config.database}_{app.config.id_ris}_load.py")

    with open(file_path, 'w') as dag:
        dag.write(dt.tmp_dag(app.config.docs,
                             app.config.developer,
                             app.config.database,
                             app.config.id_ris,
                             app.config.topic,
                             app.flow.flow))

    import re
    # Define the old and new values for the schema from config
    old_schema = r"'schema': '(\w+)'"
    new_schema = r"'schema': etl_schema"

    with open(file_path, 'r') as f:
        contents = f.read()

    # Replace the old value with the new value in the contents
    new_contents = re.sub(old_schema, new_schema, contents)

    # Write modified contents
    with open(file_path, 'w') as f:
        f.write(new_contents)