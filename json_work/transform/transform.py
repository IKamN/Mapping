from json_work.extract.extract import Extract

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

            if table not in flow.flow.keys():
                flow.append_table(table, node.alias, explodedColumns, anyOfExists)
                if flow.tab_lvl != 0:
                    flow.append_hash(table, explodedColumns)

            if hasattr(node, 'properties'):
                for key, value in node.properties.items():
                    new_path = flow.update_path(path, key)
                    attr_key = Attributes(value)
                    if len(attr_key.refs) != 0:
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
                        flow.append_columns(new_path, table, explodedColumns, attr_key.type, node, attr_key.alias, anyOfExists)
            else:
                flow.append_columns(path, table, explodedColumns, "string", node, describe_attr, anyOfExists)

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
            elif (key == "type") and (value in ["number", "integer"]):
                setattr(self, key, "bigint")
            elif (key == "type") and (value == "boolean"):
                setattr(self, key, "string")
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
        self.flow: dict = {}
        self.tab_lvl = 0

    def __prepare_table(self, start_table:str) -> str:
        import re
        tmp = start_table.split('_')[1:]
        prefix = start_table.split('_')[0]
        letters = ['b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'y', 'z']
        for i in range(0, len(tmp) - 1):
            if i == 0:
                for l in range(len(tmp[i]) - 1, 0, -1):
                    if re.search('[A-Z]', tmp[i][l]):
                        for r in range(2, 7):
                            if tmp[i][l + r] in letters:
                                tmp[i] = tmp[i][0:l + r + 1]
                                break
                        break
            else:
                parts = re.findall('[A-Z][^A-Z]*', tmp[i])
                if len(parts) > 1:
                    for k in range(0, len(parts)):
                        for l in range(0, len(parts[k])):
                            if re.search('[A-Z]', parts[k][l]):
                                for r in range(2, 7):
                                    if parts[k][r] in letters:
                                        tmp[i] = tmp[i].replace(parts[k], parts[k][0:r + 1])
                                        break
                                break
                        if len(prefix + '_' + '_'.join(tmp)) > 60:
                            continue
                        else:
                            break
                else:
                    for l in range(0, len(parts)):
                        if re.search('[A-Z]', parts[0]):
                            for r in range(3, 7):
                                if tmp[i][r] in letters:
                                    tmp[i] = tmp[i][0:r + 1]
                                    break
                            break
            if len(prefix + '_' + '_'.join(tmp)) > 60:
                continue
            else:
                break
        return prefix + '_' + '_'.join(tmp)

    def __prepare_alias(self, path:str, table_name:str) -> str:
        import re
        exist_alias = []
        if len(self.flow[table_name]["parsedColumns"]) > 3:
            exist_alias += [columns["alias"] for columns in self.flow[table_name]["parsedColumns"][4:]]

        alias = ""
        # result_string = path
        # Добавить сортировку сначала самые короткие
        # while "." in result_string:
        #     result_string = re.sub(r'^[^.]+\.', '', result_string)
        #     if path.replace(".", "_") in exist_alias:
        #         alias = re.sub(r'\.([^\.]+)$', r'\g<0>\g<1>', path).replace(".", "_")
        #         return alias
        #     if result_string.replace(".", "_") not in exist_alias:
        #         alias = result_string.replace(".", "_")
        # return alias

        if path.replace(".", "_") in exist_alias:
            alias = re.sub(r'\.([^\.]+)$', r'\g<0>\g<1>', path).replace(".", "_")
            return alias
        else:
            alias = path.lower() if len(path.split(".")) == 1 else "_".join(path.split(".")[1:]).lower()
        return alias

    def append_hash(self, table_name:str, explodedColumns:list):
        parent_table = "_".join(table_name.split("_")[:-1]) if len(table_name.split("_")) > 1 else table_name.split("_")[0]
        parent_path = explodedColumns[-1]
        parent_alias = parent_path.lower() + "_hash" if len(parent_path.split(".")) == 1 else "_".join(
            parent_path.split(".")[1:]).lower() + "_hash"
        parent_describe = f"связь с {table_name}"

        array_path = explodedColumns[-1].split(".")[-1] + "_array"
        alias_hash = array_path.replace("array", "hash")
        array_describe = f"связь с {parent_table}"


        self.flow[table_name]["parsedColumns"].insert(4,
            {"name": array_path, "colType": "hash", "alias": alias_hash, "description": array_describe}
        )

        self.flow[parent_table]["parsedColumns"] += [
            {"name": parent_path, "colType": "hash", "alias": parent_alias, "description": parent_describe}
        ]

    def append_table(self, table_name:str, describe_table:str, explodedColumns:list, anyOfExists:int) -> None:

        short_table_name = ""
        if len(table_name) > 60:
            short_table_name = self.__prepare_table(table_name)

        tech_parsedColumns = [
            {'name': 'ChangeId', 'colType': 'string', "description": "Уникальный идентификатор изменений"},
            {'name': 'ChangeType', 'colType': 'string', "description": "Тип изменений"},
            {'name': 'ChangeTimestamp', 'colType': 'string', "description": "Временная метка сообщения"},
            {'name': 'Hdp_Processed_Dttm', 'colType': 'timestamp', "description": "Дата и время внесения записи в DAPP"},
        ]

        if anyOfExists == 1:
            preFilterCondition = f"value like '%Class_:_{self.meta_class}%'"
            postFilterCondition = f"meta.Class = '{self.meta_class}'"
        else:
            preFilterCondition = f"value like '%Class_:_{self.meta_class}%' and value like '%payload.Id_:_%'"
            postFilterCondition = f"payload.Id = ''"

        if table_name not in self.flow:
            self.flow[table_name] = {
                "describe_table": describe_table,
                "explodedColumns": explodedColumns,
                "tab_lvl": self.tab_lvl,
                "parsedColumns": tech_parsedColumns,
                "parent_table": '',
                "preFilterCondition": preFilterCondition,
                "postFilterCondition": postFilterCondition,
                "short_table_name": short_table_name
            }

    def append_columns(self, path:str, table_name: str, explodedColumns:list, colType:str, node:Node, describe_attr:str, anyOfRefs:int) -> None:
        alias = self.__prepare_alias(path, table_name)
        self.flow[table_name]["parsedColumns"] += [{"name": path, "colType": colType, "alias": alias, "description": describe_attr}]


    def update_path(self, path:str, key:str) -> str:
        return path + f".{key}"


    def next_array(self, path:str, explodedColumns:list, table:str) -> dict:
        self.tab_lvl = self.tab_lvl + 1
        new_explodedColumns = []
        new_explodedColumns += explodedColumns

        if len(explodedColumns) == 1:
            new_explodedColumns.append(path)
            table = table + "_" + "".join(new_explodedColumns[-1].split(".")[1:])
        else:
            prefix = new_explodedColumns[-1].split(".")[-1]
            postfix = path.split(".")[1:]
            new_explodedColumns.append(".".join([prefix] + postfix))
            table = table + "_" + "".join(new_explodedColumns[-1].split(".")[1:])
        path = path.split(".")[-1]
        return {"path":path, "explodedColumns":new_explodedColumns, "table":table}

    def pprint(self):
        return print(self.flow)