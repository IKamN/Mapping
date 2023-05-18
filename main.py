from json_work import read_files
from config import config_file
from save_dag import write_dag
from UI import ui



if __name__ == '__main__':
    ui
    params = config_file.read_config() # return user params
    flows = read_files.read_json(params) # reading all json_work in directory, return flows for dag
    write_dag.write_file(params, flows) # save dag file
