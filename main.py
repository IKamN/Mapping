from json_work import read_files
from config import config_file
from save_dag import write_dag


# 1) Подготовка json, разложение до definitions, подготовка полей для рекурсии
# 2) Вызов рекурсонной функции
# 3) Сохранение в эксель, передача df_dag для формирования flow
# 4) Подготовка датарфейма
# 5) Сохранение дага


if __name__ == '__main__':
    params = config_file.read_config() # return user params
    flows = read_files.read_json(params) # reading all json_work in directory, return flows for dag
    write_dag.write_file(params, flows) # save dag file
