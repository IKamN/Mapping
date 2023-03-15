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

    flows = []
    for filename in os.listdir(params['file_dir']):
        if filename.endswith('json'):
            json_file = os.path.join(params['file_dir'], filename)

            # handle json, return (definitions, meta_class, nodes)
            inf = extract.handle_json(json_file)
            definitions = inf[0]
            meta_class = inf[1]
            nodes = inf[2]

            # parsing json, return dict with data
            mapping_dict = h_json.parsing_json(definitions, nodes, params['database'])


            # dfd.prepare_df() return flows for dag
            flows += dfd.prepare_df(mapping_dict, meta_class, params['tech_fields'], params['loadType'], params['colsToHash'], params['topic'], params['etl_schema'], params['file_dir'], os.path.basename(json_file) if params['old_mapping_flag'].lower() == 'yes' else None)

            # save excel mapping
            xlsx_name = os.path.join(os.path.dirname(json_file), f'S2T_mapping_{params["file_name"]}_' + os.path.basename(json_file).replace('json', 'xlsx'))
            dfe.save_excel(mapping_dict, xlsx_name, params['base_system_target'], params['base_system_source'], params['id_is'])

    return flows


