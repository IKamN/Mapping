import pandas as pd
import os
from check.config.config import check_config


# Result dataframe from ift/preprod
params = check_config()
df = pd.DataFrame()
df_raw = pd.DataFrame()
for filename in os.listdir(params['check_dir']):
    if filename.endswith('csv'):
        df = pd.read_csv(params['check_dir']+filename)
        table_name = '_'.join(filename.split('_')[:-1])
        columns = [i for i in df.columns]
        if table_name.startswith('streaming'):
            df_raw = pd.read_csv(params['check_dir']+filename)['value']
            print(df_raw)
