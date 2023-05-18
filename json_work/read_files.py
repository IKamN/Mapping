def read_json(config):
    """
    :param params: user params from config
    :return: list flows for dag
    """
    import os
    from json_work.transform.transform import Transform
    from dag_flow.dag_flow import Flow
    from json_work.S2Tmapping.S2Tmapping import Mapping
    from json_work.transform.naming import NamingPrepare

    flows = []
    for filename in os.listdir(config.file_dir):
        if filename.endswith('json'):
            json_file = os.path.join(config.file_dir, filename)

            # parsing json, return FlowProcess object
            json_data = Transform(json_file).iterate_refs(config.database)

            # transform table names, alias in parsedColumns
            rename_data = NamingPrepare(json_data)

            # Return flows list
            flows += Flow(rename_data, loadType=config.loadType, topic=config.topic).create_flow()

            # save excel mapping
            replace_version = json_file.split('_')[-1][2:-5] if len(json_file.split('_')[-1][2:-5]) > 0 else '1.0'
            xlsx_name = os.path.join(os.path.dirname(json_file), f'S2T_mapping_{config.subo_name}_' + os.path.basename(json_file).replace('json', 'xlsx')).replace(replace_version, str(config.mapping_version))
            Mapping(rename_data, config.subo_name, config.database, xlsx_name)

    return flows


