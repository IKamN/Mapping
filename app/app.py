from typing import Optional
# from config.config_file import Config, setup_config
from flow.flow import Flow, setup_flow
from airflow_dag.airflow_dag import save_airflow_dag
from ui.ui import setup_ui, Config


class App:
    config: Optional[Config]
    flow: Optional[Flow]

app: App = App()

def setup_app(config_path: str):
    # Get config parameters
    # setup_config(app, config_path)

    # Start ui and set config params
    setup_ui(app, config_path)

    # Get all paths from json file
    setup_flow(app)

    # Save airflow .py file
    save_airflow_dag(app)