import save_dag.dag_template.template as dt

def write_file(config, flows):
    """
    :param params: parameters from config
    :param flows: flows with paths for dag
    :return: create dag file in directory
    """
    import os
    file_path = os.path.join(f"{config.file_dir}", f"1642_19_datalake_{config.database}_{config.id_ris}_load.py")

    with open(file_path, 'w') as dag:
        dag.write(dt.tmp_dag(config.docs,
                             config.developer,
                             config.database,
                             config.id_ris,
                             config.topic,
                             flows))

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