import save_dag.dag_template.template as dt

def write_file(params, flows):
    """
    :param params: parameters from config
    :param flows: flows with paths for dag
    :return: create dag file in directory
    """
    import os
    file_path = os.path.join(f"{params['file_dir']}", f"1642_19_datalake_{params['database']}_{params['id_is']}_load.py")

    with open(file_path, 'w') as dag:
        dag.write(dt.tmp_dag(params['docs'],
                             params['developer'],
                             params['database'],
                             params['id_is'],
                             params['topic'],
                             params['etl_schema'],
                             flows))

    import re
    # Define the old and new values for the schema variable
    old_schema = r"'schema': '(\w+)'"
    new_schema = r"'schema': etl_schema"

    # Open the file and read its contents
    with open(file_path, 'r') as f:
        contents = f.read()

    # Replace the old value with the new value in the contents using regular expressions
    new_contents = re.sub(old_schema, new_schema, contents)

    # Write the modified contents back to the file
    with open(file_path, 'w') as f:
        f.write(new_contents)