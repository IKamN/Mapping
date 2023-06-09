import os
from app.app import setup_app

if __name__ == '__main__':
    config_path: str = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.yml')
    setup_app(config_path)
