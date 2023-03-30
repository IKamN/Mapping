def prepare_df(mapping_dict, meta_class,
               tech_fields, loadType, colsToHash, topic, etl_schema, file_dir, json_file=None):
    import pandas as pd
    import os

    df_dag = pd.DataFrame({'table_name': mapping_dict['table_name'],
                           'code_attr': mapping_dict['code_attr'],
                           'tab_lvl': mapping_dict['tab_lvl'],
                           'colType': mapping_dict['colType'],
                           'explodedColumns': mapping_dict['explodedColumns'],
                           'meta_class': meta_class,
                           'filter_condition': mapping_dict['filter_condition'],
                           'old_map':mapping_dict['old_map']})

    # prepare dataframe
    df_dag.drop(df_dag[df_dag['code_attr'].str.lower() == 'hdp_processed_dttm'].index, inplace=True)
    df_dag['alias'] = df_dag['code_attr'].apply(lambda x: x.lower().replace('array', 'hash') if x.endswith('array')
                                                else '_'.join(x.split('_')[1:] if len(x.split('_')) >= 2 else x).lower())
    df_dag.loc[df_dag['colType'] != 'hash', 'colType'] = 'string'

    for ind in df_dag.index:
        tab_lvl = df_dag.loc[ind, 'tab_lvl']
        code_attr = df_dag.loc[ind, 'code_attr']
        colType = df_dag.loc[ind, 'colType']
        if (colType == 'hash') & ('array' not in code_attr):
            df_dag.loc[ind, 'alias'] = ''.join(code_attr.split('.')[1:]).lower() + '_hash'
        # else:
        #     df_dag.loc[ind, 'alias'] = code_attr.split('_')[-1].lower()
        if tab_lvl != 0:
            # if (len(alias.split('_')) >= 2) & \
            #         (alias not in tech_fields) & \
            #         ('_hash' not in alias):
            #     df_dag.loc[ind, 'alias'] = '_'.join(alias.split('_')[2:])
            if 'hash' in code_attr:
                df_dag.loc[ind, 'code_attr'] = df_dag.loc[ind, 'explodedColumns'].split(',')[-1].split('.')[-1] + '.' + code_attr.replace('_hash', '')


    df_dag.loc[df_dag['colType'] != 'hash', 'code_attr'] = df_dag['code_attr'].apply(lambda x: x.replace('_', '.'))



    # GET OUT DUPLICATES
    # df_dag.drop_duplicates(subset=['table_name', 'code_attr', 'colType', 'alias'], inplace=True)
    # df_dag['check'] = df_dag['code_attr']
    # dup_indexes = df_dag.groupby('table_name').apply(lambda x: x[x.duplicated(subset=['alias'], keep=False)].index)
    # for table, indexes in dup_indexes.items():
    #     expl_check = []
    #     for index in indexes:
    #         if (not indexes.empty) & (df_dag.loc[index, 'explodedColumns'].split(', ')[-1] not in expl_check) & (df_dag.loc[index, 'colType'] == 'hash'):
    #             expl_check.append(df_dag.loc[index, 'code_attr'])
    #             df_dag.loc[index, 'alias'] = '_'.join(df_dag.loc[index, 'check'].split('.')[2:]).lower() + '_hash'
    #
    # # Пока костыль, доп проверка на дубли
    # dup_indexes = df_dag.groupby('table_name').apply(lambda x: x[x.duplicated(subset=['alias'], keep=False)].index)
    # for table, indexes in dup_indexes.items():
    #     for index in indexes:
    #         if (not indexes.empty) & (df_dag.loc[index, 'explodedColumns'].split(', ')[-1] not in expl_check):
    #             print('Еще остались дубли, смотреть таблицу', df_dag.loc[index, 'table_name'].split(', ')[-1],'\n',
    #                   'Путь: ',df_dag.loc[index, 'code_attr'], '\nТекущее значение: ', df_dag.loc[index, 'alias'])
    #         else:
    #             pass
                # print('Ок, дублей нет')

    # ---------------------------------------------------------------------------------------------------------------------
    # handler for old mappings

    # old_map = ''
    # old_techFields = ['COMMIT_TS', 'OP_SCN', 'OP_SEQ', 'OP_TYPE', 'PROCESSED_DT', 'DTE']
    # df_old = pd.DataFrame()
    # if json_file != None:
    #     for filename in os.listdir(file_dir):
    #         if filename.endswith('xlsx') & (filename.split('.')[0] == json_file.split('.')[0]):
    #             old_map = os.path.join(file_dir, filename)
    #         if filename.endswith('xlsx') & (filename.split('_')[-1] == json_file.replace('json', 'xlsx')):
    #             old_map = os.path.join(file_dir, filename)
    #     df_old = pd.read_excel(old_map, sheet_name='Mapping', header=1)
    #     old_attr = [i for i in df_old.columns if i in('Тэг в JSON', 'Код атрибута.1')]
    #     df_old = df_old.rename(columns={old_attr[0]:'all_tags', old_attr[1]: 'code_attr'})
    #     for i in df_old.index:
    #         if (df_old.loc[i, 'code_attr'].upper() in old_techFields) or ('guid' in df_old.loc[i, 'code_attr'].lower()):
    #             df_old.loc[i, 'all_tags'] = df_old.loc[i, 'code_attr']
    #         else:
    #             df_old.loc[i, 'all_tags'] = df_old.loc[i, 'all_tags'].replace('[]','')
    #             df_old.loc[i, 'all_tags']= '_'.join(df_old.loc[i, 'all_tags'].split('.')[1::2])
    #     df_dag.to_excel(f"{file_dir}+new_{json_file}.xlsx", sheet_name='Mapping', index=False)
    #     df_old.to_excel(f"{file_dir}+{json_file}.xlsx", sheet_name='Mapping', index=False)
        # df_old = df_old.rename(columns={df_old.columns[21]: 'table_name', df_old.columns[23]: 'code_attr'})
        # print(df_old.columns)
        # df_old['code_attr'] = df_old['code_attr'].apply(lambda row: row if row.upper() not in old_techFields else None)
        # df_old.dropna(subset=['code_attr'], inplace=True)
        # df_old.sort_values(['table_name', 'code_attr'], inplace=True)

        # test_dag = df_dag
        # test_dag['code_attr'] = df_dag['code_attr'].apply(lambda row: row if (row not in tech_fields) & ('hash' not in row) & ('array' not in row) else None)
        # test_dag.dropna(subset=['code_attr'], inplace=True)
        # test_dag.sort_values(['table_name', 'old_map'], inplace=True)
        # test_dag[['table_name', 'old_map']].to_excel(f"{file_dir}+{json_file}.xlsx", sheet_name='Mapping', index=False)
        # df_old[['table_name', 'code_attr']].to_excel(f"{file_dir}{json_file.replace('json', 'xlsx')}", sheet_name='Mapping', index=False)
        # df = pd.DataFrame(mapping_dict).sort_values(by=['table_name', 'code_attr'])
    # ---------------------------------------------------------------------------------------------------------------------

    df_dag['paths'] = df_dag.apply(
        lambda x: {'name': x['code_attr'], 'colType': x['colType'], 'alias': x['alias']} if x['code_attr'] not in tech_fields
        else {'name': x['code_attr'], 'colType': x['colType']}, axis=1)

    if df_dag['filter_condition'].nunique() > 1:
        df_dag['preFilterCondition'] = df_dag['meta_class'].apply(lambda x: "value like \'%meta_:{_Class_:_" + x + "_%\' and value like \'%Id_:_") + df_dag['filter_condition'].apply(lambda x: x+"%\'")
        df_dag['postFilterCondition'] = df_dag['filter_condition'].apply(lambda x: "payload.Id = \'" + x +"\'")
    else:
        df_dag['preFilterCondition'] = df_dag['meta_class'].apply(lambda x: "value like \'%meta_:{_Class_:_" + x + "_%\'")
        df_dag['postFilterCondition'] = df_dag['meta_class'].apply(lambda x: "meta.Class = \'"+ x+"\'")

    grouped = df_dag.groupby(['table_name', 'explodedColumns', 'preFilterCondition', 'postFilterCondition'])['paths'].apply(list).reset_index()

    flows = [
        {
            "loadType": loadType,
            "source": {
                "schema": etl_schema,
                "table": f'streaming_smart_replication_change_request_{topic}_default',
                "columnsWithJson": ["value"],
                "explodedColumns": explodedColumns.split(', '),
                "parsedColumns": paths,
                "preFilterCondition": preFilterCondition,
                "postFilterCondition": postFilterCondition,
                "incrementField": "hdp_processed_dttm"
            },
            'target': {
                'table': table_name,
                'colsToHash': colsToHash
            } if loadType.lower() != 'scd0append' else
            {
                'table': table_name
            }
            ,
            'addInfo': {'orderField': 'changeTimestamp'}
        }
        for table_name, paths, explodedColumns, preFilterCondition, postFilterCondition in
        zip(grouped['table_name'], grouped['paths'], grouped['explodedColumns'], grouped['preFilterCondition'], grouped['postFilterCondition'])
    ]

    return flows
