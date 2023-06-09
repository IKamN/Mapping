from dataclasses import dataclass
import typing
import yaml


if typing.TYPE_CHECKING:
    from app.app import App

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


def setup_config(app:"App", config_path:str):
    """
    Read config yml file. Fill application config params
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        raw = yaml.safe_load(f)

    app.config = Config(
        file_dir=raw['file_dir'],
        subo_name=raw['subo_name'],
        system_target=raw['system_target'],
        docs=raw['docs'],
        developer=raw['developer'],
        id_ris=raw['id_ris'],
        loadType=raw['loadType'],
        database=raw['database'],
        topic=raw['topic'],
        mapping_version=raw['mapping_version']
    )