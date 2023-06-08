from json_work.extract.extract import Extract
from json_work.transform.schemes import Table, ParsedColumns, TableAttributes, Flow


class Transform(Extract):
    def iterate_refs(self, database:str):
        json_file = self.open_json()
        payload_refs = json_file['payload']
        definitions = json_file['definitions']
        meta_class = json_file['meta']
        anyOfExists = len(payload_refs)
        flow = FlowProcessing(meta_class)

        def listing_definitions(ref, table, path, explodedColumns, describe_attr:str) -> None:
            node = Node(definitions[ref])
            table = table.lower().replace(".", "_")

            if not any(t.table_name == table for t in flow.new_flow.tables):
                flow.append_table(table, node.alias, explodedColumns, anyOfExists)
                if flow.tab_lvl != 0:
                    flow.append_hash(table, explodedColumns)

            if hasattr(node, 'properties'):
                for key, value in node.properties.items():
                    new_path = flow.update_path(path, key)
                    attr_key = Attributes(value)
                    cnt_refs = len(attr_key.refs)
                    if cnt_refs != 0:
                        for ref in attr_key.refs:
                            if (hasattr(attr_key, "type")) and (attr_key.type == 'array'):
                                updated = flow.next_array(new_path, explodedColumns, table)
                                array_table = updated["table"]
                                array_explodedColumns = updated["explodedColumns"]
                                array_path = updated["path"]
                                listing_definitions(ref, array_table, array_path, array_explodedColumns, attr_key.alias)
                                flow.tab_lvl -= 1
                            else:
                                listing_definitions(ref, table, new_path, explodedColumns, attr_key.alias)
                    else:
                        flow.append_columns(new_path, table, attr_key.type, attr_key.alias, anyOfExists)
            else:
                flow.append_columns(path, table, "string", describe_attr, anyOfExists)

        for start_table in payload_refs:
            start_path = "payload"
            ref = start_table
            start_table = f"{database}_{start_table}"
            explodedColumns = ["payload"]
            describe_attr = ""
            listing_definitions(ref, start_table, start_path, explodedColumns, describe_attr)
        return flow



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


class Node:
    def __init__(self, node_attr: dict):
        for key, value in node_attr.items():
            if key == "title":
                setattr(self, "alias", value)
            else:
                setattr(self, key, value)
        if ("alias" not in node_attr) and ("title" not in node_attr):
            setattr(self, "alias", "")


class FlowProcessing:

    def __init__(self, meta_class):
        self.meta_class = meta_class
        self.tab_lvl = 0
        self.new_flow = Flow(tables=[])

    def append_hash(self, table_name:str, explodedColumns:list):
        last_array = ".".join(explodedColumns[-1].split(".")[1:]).lower()
        parent_table = table_name.replace(f"_{last_array}", "")
        descr_table = self.new_flow.find_table(table_name).describe_table
        # print(f"table_name:{table_name}, parent_table:{parent_table}, explodedColumns:{explodedColumns}")
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

    def append_table(self, table_name:str, describe_table:str, explodedColumns:list, anyOfExists:int) -> None:
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

        if anyOfExists == 1:
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

    def append_columns(self, path:str, table_name: str, colType:str, describe_attr:str, anyOfRefs:int) -> None:
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

    def pprint(self):
        return print(self.flow)
