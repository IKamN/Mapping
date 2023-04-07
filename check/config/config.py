def check_config():
    import yaml
    import os

    config_path = os.path.join(os.path.dirname(os.path.realpath(__package__)), '../df_excel/config.yml')
    with open(config_path, 'r', encoding='utf-8') as f:
        raw = yaml.safe_load(f)

        params = {
            'check_dir': raw['check_dir']
        }

    return params