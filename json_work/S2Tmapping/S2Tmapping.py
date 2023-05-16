def Mapping(data:dict, base_system_source:str, database:str, file_name:str) -> None:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    import re

    # create new book excel
    wb = openpyxl.Workbook()

    # Create new execel list
    ws = wb.active
    ws.append(["start"])

    ws.column_dimensions['A'].width = 3
    ws.column_dimensions['B'].width = 15
    ws.column_dimensions['C'].width = 15

    # Create headers
    headers = ["#", "Тип объекта",
               "База/Система", "Тэг в JSON", "Таблица", "Название родительской таблицы", "Описание таблицы", "tab_lvl", "Код атрибута", "Краткое описание таблицы", "Тип данных","Длина", "PK", "FK", "Not Null", "Status", "Version",
               "База/Система", "Схема", "Таблица", "Название родительской таблицы", "Описание таблицы", "tab_lvl", "Код атрибута", "Описание атрибута", "Комментарий", "Тип данных", "Length", "PK", "FK", "Not Null", "Rejectable", "Trace New Values",]
    ws.append(headers)

    index = 1
    for table_name, table_data in data.items():
        source_table = re.sub(r'^.*?_', '', table_name)
        schema = f"prod_repl_subo_{database}"
        for column_data in table_data['parsedColumns']:
            tag_name = ""
            if "array" in column_data["name"]:
                tag_name = column_data["name"].replace("array", "hash")
            elif "." not in column_data["name"]:
                tag_name = column_data["name"]
            else:
                tag_name = ".".join(column_data["name"].split(".")[1:]) if column_data["colType"] != "hash" else ".".join(column_data["name"].split(".")[1:]) + "_hash"

            tag_json = f'{source_table.replace("_", ".")}[].{tag_name}' if column_data["colType"] != "hash" else "New_hash"
            column_data["colType"] = "string" if column_data["colType"] == "hash" else column_data["colType"]
            parent_table = "_".join(table_name.split("_")[:-1]) if table_data["tab_lvl"] != 0 else ""
            if "alias" in column_data:
                row_data = [index, "Реплика", base_system_source, tag_json, source_table, parent_table, table_data["describe_table"],
                            table_data["tab_lvl"], tag_name, column_data["description"], column_data["colType"], "", "", "", "", "", "",
                            "1642_19 Озеро данных", schema, table_name, parent_table, table_data["describe_table"], table_data["tab_lvl"],
                            column_data["alias"], column_data['description'], "", column_data['colType']]
            else:
                row_data = [index, "Реплика", base_system_source, column_data['name'], source_table, parent_table,
                            table_data["describe_table"], table_data["tab_lvl"], "",
                            "", column_data["colType"], "", "", "", "", "", "",
                            "1642_19 Озеро данных", schema, table_name, parent_table,  table_data["describe_table"], table_data["tab_lvl"],
                            column_data["name"], column_data['description'], "", column_data['colType']]
            index += 1
            ws.append(row_data)

    for col in ws.columns:
        max_length = 0
        column = col[1:]
        for cell in column:
            if len(str(cell.value)) > max_length:
                max_length = len(str(cell.value))

        adjusted_width = (max_length + 2)
        col_letter = col[0].column_letter
        ws.column_dimensions[col_letter].width = adjusted_width

    fiolet_fill = PatternFill(start_color='B2AFE0', end_color='B2AFE0', fill_type='solid')
    orange_fill = PatternFill(start_color='ffd966', end_color='ffd966', fill_type='solid')
    blue_fill = PatternFill(start_color='A5C4E0', end_color='A5C4E0', fill_type='solid')
    green_fill = PatternFill(start_color='aed59d', end_color='aed59d', fill_type='solid')

    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    for col in range(1, 3):
        for cell in ws[f"A{col}":f"B{col}"][0]:
            cell.fill = fiolet_fill
        for cell in ws[f"R{col}":f"AG{col}"][0]:
            cell.fill = blue_fill

        for cell in ws[f"A{col}:AG{col}"][0]:
            cell.border = border

    for cell in ws[f"C{1}":f"Q{1}"][0]:
        cell.fill = orange_fill
    for cell in ws[f"C{2}":f"Q{2}"][0]:
        cell.fill = green_fill


    ws.merge_cells("A1:B1")
    ws["A1"] = "Source/Target"

    ws.merge_cells("C1:Q1")
    ws["C1"] = "Source"

    ws.merge_cells("R1:AG1")
    ws["R1"] = "Target"

    # Выравнивание текста по центру в объединенных ячейках
    center_alignment = Alignment(horizontal='center', vertical='center')
    ws['A1'].alignment = center_alignment
    ws['C1'].alignment = center_alignment
    ws['R1'].alignment = center_alignment

    wb.save(f"{file_name}.xlsx")