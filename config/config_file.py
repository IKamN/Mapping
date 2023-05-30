from dataclasses import dataclass
import yaml


@dataclass
class Config:
    file_dir:str
    subo_name:str
    id_ris:str
    loadType:str
    mapping_version:str
    database:str
    topic:str
    system_target:str
    developer:str
    docs:str


def setup_config(config_path:str):

    with open(config_path, 'r', encoding='utf-8') as f:
        raw = yaml.safe_load(f)

    config = Config(
        file_dir=raw['file_dir'],
        subo_name= raw['subo_name'],
        system_target=raw['system_target'],
        docs= raw['docs'],
        developer= raw['developer'],
        id_ris= raw['id_ris'],
        loadType= raw['loadType'],
        database= raw['database'],
        topic= raw['topic'],
        mapping_version=raw['mapping_version']
    )
    return config
#
#     return config
#         params = {
#             'file_dir': raw['file_dir'],
#             'file_name': raw['file_name'],
#             'base_system_source': raw['base_system_source'],
#             'base_system_target': raw['base_system_target'],
#             'docs': raw['docs'],
#             'developer': raw['developer'],
#             'id_is': raw['id_is'],
#             'loadType': raw['loadType'],
#             'database': raw['database'],
#             'topic': raw['topic'],
#             'mapping_version': raw['mapping_version']
#         }
#
#     return params



# def setup_config(config_path:str):
#     import yaml
#     import os
#
#     # config_path = os.path.join(os.path.dirname(os.path.realpath(__package__)), 'config.yml')
#     with open(config_path, 'r', encoding='utf-8') as f:
#         raw = yaml.safe_load(f)
#
#         params = {
#             'file_dir': raw['file_dir'],
#             'file_name': raw['file_name'],
#             'base_system_source': raw['base_system_source'],
#             'base_system_target':raw['base_system_target'],
#             'docs': raw['docs'],
#             'developer': raw['developer'],
#             'id_is': raw['id_is'],
#             'loadType': raw['loadType'],
#             'database': raw['database'],
#             'topic': raw['topic'],
#             'mapping_version' :raw['mapping_version']
#         }
#
#     return params