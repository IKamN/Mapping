import pandas as pd

file = 'D:\Projects\VTB\Mapping_files\\1387\\test\S2T_mapping_СУБО_Переводы_ФЛ_RetailMoneyTransferOrder_v.3_DAPP.xlsx'

df = pd.read_excel(file, sheet_name='Mapping', header=1)[['Таблица.1','Код атрибута.1']]
df.reset_index(inplace=True)
# print(df)


df_dup = df[df.duplicated(subset=['Таблица.1','Код атрибута.1'], keep=False)]
# print(df_dup)

print(len('source_retailmoneylocationfps_search_search_retailmoneylocationsearchmethodbankandaccount_type'))
# for i, val in df.iterrows():
#     if len(val[1]) >= 128:
#         print(i)