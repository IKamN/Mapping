def prepare_df(mapping_dict, meta_class,
               tech_fields, loadType, colsToHash, topic, etl_schema):
    import pandas as pd

    df_dag = pd.DataFrame({'table_name': mapping_dict['table_name'],
                           'code_attr': mapping_dict['code_attr'],
                           'tab_lvl':mapping_dict['tab_lvl'],
                           'colType':mapping_dict['colType'],
                           'explodedColumns':mapping_dict['explodedColumns'],
                           'meta_class': meta_class,
                           'filter_condition': mapping_dict['filter_condition']})

    # prepare dataframe
    df_dag.drop(df_dag[df_dag['code_attr'].str.lower() == 'hdp_processed_dttm'].index, inplace=True)
    df_dag['alias'] = df_dag['code_attr'].apply(lambda x: x.lower().replace('array', 'hash') if x.endswith('array') else x.lower())
    df_dag.loc[df_dag['alias'].str.endswith(('_hash', '_array')), 'colType'] = 'hash'
    df_dag.loc[df_dag['colType'] != 'hash', 'colType'] = 'string'

    for ind in df_dag[['alias', 'tab_lvl']].index:
        alias = df_dag.loc[ind, 'alias']
        tab_lvl = df_dag.loc[ind, 'tab_lvl']
        if tab_lvl != 0:
            if (len(alias.split('_')) > 2) & \
                    (alias not in tech_fields) & \
                    ('_hash' not in alias):
                df_dag.loc[ind, 'alias'] = '_'.join(alias.split('_')[1:])

    df_dag.loc[
        (df_dag['tab_lvl'] == 0) &
        ~(df_dag['code_attr'].str.lower().isin(tech_fields)) &
        (df_dag['colType'] != 'hash'), 'code_attr'
    ] = 'payload.' + df_dag['code_attr']

    df_dag.loc[df_dag['colType'] != 'hash', 'code_attr'] = df_dag['code_attr'].apply(lambda x: x.replace('_', '.'))

    df_dag['paths'] = df_dag.apply(
        lambda x: {'name': x['code_attr'], 'colType': x['colType'], 'alias': x['alias']} if x['code_attr'] not in tech_fields
        else {'name': x['code_attr'], 'colType': x['colType']}, axis=1)

    df_dag.drop_duplicates(subset=['table_name','code_attr', 'colType', 'alias'], inplace=True)


    if df_dag['filter_condition'].nunique() > 1:
        df_dag['preFilterCondition'] = df_dag['meta_class'].apply(lambda x: "value like \'%meta_:{_Class_:_" + x + "_}%\' and value like \'%Id_:_") + df_dag['filter_condition'].apply(lambda x: x+"%\'")
        df_dag['postFilterCondition'] = df_dag['filter_condition'].apply(lambda x: "payload.Id = \'" + x +"\'")
    else:
        df_dag['preFilterCondition'] = df_dag['meta_class'].apply(lambda x: "value like \'%meta_:{_Class_:_" + x + "_}%\'")
        df_dag['postFilterCondition'] = df_dag['meta_class'].apply(lambda x: "meta.Class = \'"+ x+"\'")


    grouped = df_dag.groupby(['table_name', 'explodedColumns', 'preFilterCondition', 'postFilterCondition'])['paths'].apply(list).reset_index()


    flows = [
        {
            "loadType": loadType,
            "source": {
                "schema": etl_schema,
                "table": f'streaming_smart_replication_change_request_{topic}_default',
                "columnsWithJson": ["value"],
                "explodedColumns": [explodedColumns],
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
