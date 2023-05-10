def read_json(params):
    """
    :param params: user params from config
    :return: list flows for dag
    """
    import os
    from json_work.transform.transform import Transform
    from dag_flow.dag_flow import Flow

    flows = []
    for filename in os.listdir(params['file_dir']):
        if filename.endswith('json'):
            json_file = os.path.join(params['file_dir'], filename)

            # parsing json, return FlowProcess object
            json_data = Transform(json_file).iterate_refs()

            tech_fields = params['tech_fields']
            loadType = params['loadType']
            colsToHash = params['colsToHash']

            topic = params['topic']
            file_dir = params['file_dir']

            # Return flows list
            flows += Flow(json_data, loadType=loadType, topic=topic, colsToHash=colsToHash).create_flow()

            # print(flows)
            # save excel mapping
            # replace_version = json_file.split('_')[-1][2:-5] if len(json_file.split('_')[-1][2:-5]) > 0 else '1.0'
            # xlsx_name = os.path.join(os.path.dirname(json_file), f'S2T_mapping_{params["file_name"]}_' + os.path.basename(json_file).replace('json', 'xlsx')).replace(replace_version, str(params['mapping_version']))
            # dfe.save_excel(mapping_dict, xlsx_name, params['base_system_target'], params['base_system_source'], params['id_is'], params['database'])

    return flows


