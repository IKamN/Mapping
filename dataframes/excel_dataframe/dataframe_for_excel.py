def save_excel(mapping_dict, xlsx_name, base_system_target,
               base_system_source, id_is):
    import pandas as pd

    tech_fields = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
    del mapping_dict['explodedColumns']
    del mapping_dict['filter_condition']

    df = pd.DataFrame(mapping_dict).sort_values(by=['table_name'])
    df['code_attr']=df['code_attr'].apply(lambda x: x.replace('array', 'hash') if '_array' in x else x)

    for ind in df[['code_attr', 'tab_lvl']].index:
        code_attr = df.loc[ind, 'code_attr']
        tab_lvl = df.loc[ind, 'tab_lvl']
        if tab_lvl != 0:
            if (len(code_attr.split('_')) >= 2) & \
                    (code_attr not in tech_fields) & \
                    ('_hash' not in code_attr.lower()):
                df.loc[ind, 'code_attr'] = '_'.join(code_attr.split('_')[1:])


    df.reset_index(inplace=True)
    df.insert(1, 'База/Система1', base_system_target)
    df['Length'] = ""
    df['PK1'] = ""
    df['FK1'] = ""
    df['Not Null1'] = ''
    df["Rejectable"] = ""
    df["Trace New Values"] = ""

    df['index'] = pd.RangeIndex(start=1, stop=len(df) + 1, step=1)

    df.rename(columns={'index': '#', 'table_name': 'Таблица1', 'code_attr': 'Код атрибута1', 'describe_attr': 'Описание атрибута1',
                       'describe_table': 'Описание таблицы1', 'comment': 'Комментарий1', 'colType': 'Тип данных1'}, inplace=True)
    df.insert(1, 'Тип объекта', 'Реплика')

    def insert_row(row, number):
        if (row['Код атрибута1'] in tech_fields) or ('hash' in row['Код атрибута1']):
            return '-'
        else:
            if number == '2':
                return id_is
            elif number == '3':
                return base_system_source
            elif number == '4':
                return ''
            elif number == '5':
                return row['Таблица1']
            elif number == '6':
                return row['Описание таблицы1']
            elif number == '7':
                return row['Код атрибута1'].replace('_', '.')
            elif number == '8':
                return row['Описание атрибута1']
            elif number == '9':
                return row['Тип данных1']
            elif number == '10':
                return ''
            elif number == '11':
                return ''
            elif number == '12':
                return ''
            elif number == '13':
                return ''
            elif number == '14':
                return ''
            elif number == '15':
                return ''
            elif number == '16':
                return ''
            elif number == '17':
                return ''
            elif number == '18':
                return ''

    df.insert(2, 'ID РИС', df.apply(insert_row, axis=1, args=('2')))
    df.insert(3, 'База/Система', df.apply(insert_row, axis=1, args=('3')))
    df.insert(4, 'Схема', df.apply(insert_row, axis=1, args=('4')))
    df.insert(5, 'Таблица', df.apply(insert_row, axis=1, args=('5')))
    df.insert(6, 'Описание таблицы', df.apply(insert_row, axis=1, args=('6')))
    df.insert(7, 'Код атрибута', df.apply(insert_row, axis=1, args=('7')))
    df.insert(8, 'Краткое описание атрибута', df.apply(insert_row, axis=1, args=('8')))
    df.insert(9, 'Тип данных', df.apply(insert_row, axis=1, args=('9')))
    df.insert(10, 'Длина', df.apply(insert_row, axis=1, args=['10']))
    df.insert(11, 'PK', df.apply(insert_row, axis=1, args=['11']))
    df.insert(12, 'FK', df.apply(insert_row, axis=1, args=['12']))
    df.insert(13, 'Not Null', df.apply(insert_row, axis=1, args=['13']))
    df.insert(14, 'Dataset', df.apply(insert_row, axis=1, args=['14']))
    df.insert(15, 'Algorithm', df.apply(insert_row, axis=1, args=['15']))
    df.insert(16, 'Comment', df.apply(insert_row, axis=1, args=['16']))
    df.insert(17, 'Status', df.apply(insert_row, axis=1, args=['17']))
    df.insert(18, 'Version', df.apply(insert_row, axis=1, args=['18']))


    with pd.ExcelWriter(f'{xlsx_name}', engine='openpyxl', mode='w') as writer:
        import openpyxl
        df.to_excel(writer, sheet_name='Mapping', startrow=1, index = False)
        workbook = writer.book
        worksheet = writer.sheets['Mapping']

        # Add the headers to the first row
        worksheet.cell(row=1, column=1).value = 'Source/Target'
        worksheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=2)

        worksheet.cell(row=1, column=3).value = 'Source'
        worksheet.merge_cells(start_row=1, start_column=3, end_row=1, end_column=19)

        worksheet.cell(row=1, column=20).value = 'Target'
        worksheet.merge_cells(start_row=1, start_column=20, end_row=1, end_column=33)

        for i, col in enumerate(df.columns):
            column_length = max(df[col].astype(str).map(len).max(), len(col))
            worksheet.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = column_length + 1