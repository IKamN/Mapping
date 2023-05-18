from json_work import read_files
from save_dag import write_dag
from interface import interface
import os



if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.yml')
    config = interface.get_interface(config_path)
    flows = read_files.read_json(config) # reading all json_work in directory, return flows for dag
    write_dag.write_file(config, flows) # save dag file
