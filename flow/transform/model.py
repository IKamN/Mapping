from flow.transform.schemes import Table, ParsedColumns, TableAttributes, TableData

class Node:
    def __init__(self, node_attr: dict):
        for key, value in node_attr.items():
            if key == "title":
                setattr(self, "alias", value)
            else:
                setattr(self, key, value)
        if ("alias" not in node_attr) and ("title" not in node_attr):
            setattr(self, "alias", "")


class Attributes:
    def __init__(self, properties_dict: dict):
        for key, value in properties_dict.items():
            if key == "title":
                setattr(self, "alias", value)
            elif key == "$ref":
                setattr(self, "ref", value)
            else:
                setattr(self, key, value)

        if "type" not in properties_dict:
            setattr(self, "type", "string")
        if ("alias" not in properties_dict) and ("title" not in properties_dict):
            setattr(self, "alias", "")
        self.refs = []

        if (hasattr(self, "type")) and (self.type == "array"):
            if 'anyOf' in self.items:
                self.refs += [i['$ref'].split('/')[-1] for i in self.items['anyOf']]
            elif '$ref' in self.items:
                self.refs.append(self.items['$ref'].split('/')[-1])
        elif hasattr(self, 'anyOf'):
            self.refs += [i['$ref'].split('/')[-1] for i in self.anyOf]
        elif hasattr(self, 'ref'):
            self.refs.append(self.ref.split('/')[-1])
        else:
            pass

    def items(self):
        return self.__dict__.items()


class Transform:
    def __init__(self, meta_class: str):
        self.meta_class: str = meta_class
        self.tab_lvl: int = 0
        self.new_flow: TableData = TableData(tables=[])

    def append_hash(self, table_name:str, explodedColumns:list):
        last_array = ".".join(explodedColumns[-1].split(".")[1:]).lower()
        parent_table = table_name.replace(f"_{last_array}", "")
        descr_table = self.new_flow.find_table(table_name).describe_table

        descr_parent_table = self.new_flow.find_table(parent_table).describe_table
        parent_path = explodedColumns[-1]
        parent_alias = parent_path + ".hash" if len(parent_path.split(".")) == 1 else ".".join(parent_path.split(".")[1:]) + ".hash"

        array_path = explodedColumns[-1].split(".")[-1] + "_array"
        alias_hash = array_path.replace("_array", "_hash")

        parent_comment = f"Поле для связи с дочерней таблицей {table_name}"
        array_comment = f"Поле для связи с родительской таблицей {parent_table}"

        hash_columns = {"name": parent_path, "colType": "hash", "alias": parent_alias, "description": descr_table, "comment": parent_comment}
        array_columns = {"name": array_path, "colType": "hash", "alias": alias_hash, "description": descr_parent_table, "comment": array_comment}
        self.new_flow.append_attr(table_name,  parent_table=parent_table, parsedColumns=array_columns, flag="insert")
        self.new_flow.append_attr(parent_table, parsedColumns=hash_columns)

    def append_table(self, table_name:str, describe_table:str, explodedColumns:list, anyOfExists:bool) -> None:
        tech_parsedColumns = [
            {'name': 'changeId', 'colType': 'string', "description": "Уникальный идентификатор изменений"},
            {'name': 'changeType', 'colType': 'string', "description": "Тип изменений"},
            {'name': 'changeTimestamp', 'colType': 'string', "description": "Временная метка сообщения"},
            {'name': 'hdp_processed_dttm', 'colType': 'timestamp', "description": "Дата и время внесения записи в DAPP"},
        ]
        parsed_object = []
        for item in tech_parsedColumns:
            parsed_rows = ParsedColumns(
                name=item["name"],
                colType=item["colType"],
                description=item["description"]
            )
            del parsed_rows.alias
            parsed_object.append(parsed_rows)

        if not anyOfExists:
            preFilterCondition = f"value like '%Class_:_{self.meta_class}%'"
            postFilterCondition = f"meta.Class = '{self.meta_class}'"
        else:
            preFilterCondition = f"value like '%Class_:_{self.meta_class}%' and value like '%Type_:_%'"
            postFilterCondition = f"payload.Type = ''"

        table_attr = TableAttributes(
            explodedColumns=explodedColumns,
            parsedColumns=parsed_object
        )

        new_table = Table(
            table_name=table_name,
            attributes=table_attr,
            describe_table=describe_table,
            tab_lvl=self.tab_lvl,
            preFilterCondition=preFilterCondition,
            postFilterCondition=postFilterCondition
        )

        self.new_flow.tables.append(new_table)

    def append_columns(self, path:str, table_name: str, colType:str, describe_attr:str, anyOfRefs:int, hash_flag:str = None) -> None:
        table_name = table_name.lower().replace(".", "_")
        self.new_flow.append_attr(table_name, parsedColumns={"name": path, "colType": colType, "alias": path, "description": describe_attr, "comment": ""})

    def update_path(self, path:str, key:str) -> str:
        return path + f".{key}"

    def next_array(self, path:str, explodedColumns:list, table:str) -> dict:
        self.tab_lvl = self.tab_lvl + 1
        new_explodedColumns = []
        new_explodedColumns += explodedColumns

        if len(explodedColumns) == 1:
            new_explodedColumns.append(path)
            table = table + "_" + ".".join(new_explodedColumns[-1].split(".")[1:])
        else:
            prefix = new_explodedColumns[-1].split(".")[-1]
            postfix = path.split(".")[1:]
            new_explodedColumns.append(".".join([prefix] + postfix))
            table = table + "_" + ".".join(new_explodedColumns[-1].split(".")[1:])
        path = path.split(".")[-1]
        return {"path": path, "explodedColumns": new_explodedColumns, "table": table}
