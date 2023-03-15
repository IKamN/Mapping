import save_dag.dag_template.template as dt

def write_file(params, flows):
    """
    :param params: parameters from config
    :param flows: flows with paths for dag
    :return: create dag file in directory
    """
    import os
    with open(os.path.join(f"{params['file_dir']}", f"1642_19_datalake_{params['database']}_{params['id_is']}_load.py"), 'w') as dag:
        dag.write(dt.tmp_dag(params['docs'],
                             params['developer'],
                             params['database'],
                             params['id_is'],
                             params['topic'],
                             params['etl_schema'],
                             flows))