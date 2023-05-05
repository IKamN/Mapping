from open_json.json_schema import Extract
from json_data.json_data import Json_data

def read_json(params):
    """
    :param params: user params from config
    :return: list flows for dag
    """
    import os
    from json_work.open_json import extract
    from json_work.handle_json import h_json
    from dataframes.excel_dataframe import dataframe_for_excel as dfe
    from dataframes.dag_dataframe import dataframe_for_dag as dfd
    from dag_flow.dag_flow import Flow

    flows = []
    for filename in os.listdir(params['file_dir']):
        if filename.endswith('json'):
            json_file = os.path.join(params['file_dir'], filename)

            # handle json, return (definitions, meta_class, nodes)
            json_schema = Extract(json_file).open_json()

            inf = extract.handle_json(json_file)
            definitions = inf[0]
            meta_class = inf[1]
            nodes = inf[2]

            tech_fields = params['tech_fields']
            loadType = params['loadType']
            colsToHash = params['colsToHash']

            topic = params['topic']
            file_dir = params['file_dir']

            # parsing json, return dict with data
            json_data = Json_data()
            json_data = h_json.parsing_json(definitions, nodes, params['database'])


            # Return flows list
            flows += Flow(json_data, loadType=loadType, topic=topic, colsToHash=colsToHash).create_flow()

            # save excel mapping
            # replace_version = json_file.split('_')[-1][2:-5] if len(json_file.split('_')[-1][2:-5]) > 0 else '1.0'
            # xlsx_name = os.path.join(os.path.dirname(json_file), f'S2T_mapping_{params["file_name"]}_' + os.path.basename(json_file).replace('json', 'xlsx')).replace(replace_version, str(params['mapping_version']))
            # dfe.save_excel(mapping_dict, xlsx_name, params['base_system_target'], params['base_system_source'], params['id_is'], params['database'])

    return flows


