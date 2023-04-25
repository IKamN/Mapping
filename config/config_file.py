def read_config():
    import yaml
    import os

    config_path = os.path.join(os.path.dirname(os.path.realpath(__package__)), 'config.yml')
    with open(config_path, 'r', encoding='utf-8') as f:
        raw = yaml.safe_load(f)

        params = {
            'file_dir': raw['file_dir'],
            'old_mapping_flag':raw['old_mapping_flag'],
            'file_name': raw['file_name'],
            'base_system_source': raw['base_system_source'],
            'base_system_target':raw['base_system_target'],
            'tech_fields': raw['tech_fields'],
            'docs': raw['docs'],
            'developer': raw['developer'],
            'etl_schema': raw['etl_schema'],
            'id_is': raw['id_is'],
            'loadType': raw['loadType'],
            'database': raw['database'],
            'topic': raw['topic'],
            'colsToHash': raw['colsToHash'],
            'mapping_version' :raw['mapping_version']
        }

    return params