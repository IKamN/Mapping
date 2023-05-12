from json_work.extract.extract import Extract

class Transform(Extract):
    def iterate_refs(self, database:str):
        json_file = self.open_json()
        payload_refs = json_file['payload']
        definitions = json_file['definitions']
        meta_class = json_file['meta']
        flow = FlowProcessing(meta_class)

        def listing_definitions(ref, table, path, explodedColumns, describe_attr:str) -> None:
            node = Node(definitions[ref])

            # if table in payload_refs:
            #     flow.append_(path, table, explodedColumns, attr_key.type, node, attr_key.alias)

            if hasattr(node, 'properties'):
                # node.filter()
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
                        flow.append_(new_path, table, explodedColumns, attr_key.type, node, attr_key.alias)
            else:
                flow.append_(f"{path}.{ref}", table, explodedColumns, "string", node, describe_attr)

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
            else:
                self.refs.append('smthMissed..')
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

    def filter(self):
        filter_properties = {k: v for k, v in self.properties.items() if 'type' in v and v['type'] == 'array'}
        self.properties = {k: v for k, v in self.properties.items() if 'type' not in v or v['type'] != 'array'}
        self.properties.update(filter_properties)




class FlowProcessing:

    def __init__(self, meta_class):
        self.meta_class = meta_class
        self.flow: dict = {}
        self.tab_lvl = 0

    def prepare_columns(self, path: str, explodedColumns:list, table_name:str, colType:str, describe_attr:str, node) -> list:
        import re
        alias = path.lower() if len(path.split(".")) == 1 else "_".join(path.split(".")[1:]).lower()

        if not self.flow[table_name]["parsedColumns"]:
            tech_parsedColumns = [
                {'name': 'ChangeId', 'colType': 'string', "description": "Техническое поле"},
                {'name': 'ChangeType', 'colType': 'string', "description": "Техническое поле"},
                {'name': 'ChangeTimestamp', 'colType': 'string', "description": "Техническое поле"}
            ]
            self.flow[table_name]["parsedColumns"] += tech_parsedColumns

            # Add hash fields
            parent_table = "_".join(table_name.split("_")[:-1]) if len(table_name.split("_")) > 1 else table_name.split("_")[0]
            if self.tab_lvl != 0:
                parent_path = explodedColumns[-1]
                parent_alias = parent_path.lower() + "_hash" if len(parent_path.split(".")) == 1 else "_".join(
                    parent_path.split(".")[1:]).lower() + "_hash"
                parent_describe = describe_attr + f" (связь с {table_name})" if len(describe_attr) > 0 else f"связь с {table_name}"

                array_path = explodedColumns[-1].split(".")[-1] + "_array"
                alias_hash = array_path.replace("array", "hash")
                array_describe = describe_attr + f" (связь с {parent_table})" if len(describe_attr) > 0 else f"связь с {parent_table}"


                self.flow[table_name]["parsedColumns"] += [
                    {"name": array_path, "colType": "hash", "alias": alias_hash, "description": array_describe}
                ]
                self.flow[parent_table]["parsedColumns"] += [
                    {"name": parent_path, "colType": "hash", "alias": parent_alias, "description": parent_describe}
                ]
            self.flow[table_name]["parent_table"] = parent_table
            self.flow[table_name]["parsedColumns"] += [{"name": path, "colType": colType, "alias": alias, "description": describe_attr}]
        else:
            self.flow[table_name]["parsedColumns"] += [{"name": path, "colType": colType, "alias": alias, "description": describe_attr}]


    def append_(self, path:str, table_name: str, explodedColumns:list, colType:str, node:Node, describe_attr:str) -> None:

        if self.tab_lvl == 1:
            parent_table = "_".join(table_name.split("_")[:-1]) if len(table_name.split("_")) > 1 else table_name.split("_")[0]
            if parent_table not in self.flow:
                self.flow[parent_table] = {
                    "describe_table": node.alias,
                    "explodedColumns": explodedColumns,
                    "tab_lvl": self.tab_lvl,
                    "parsedColumns": [],
                    "parent_table": '',
                    "preFilterCondition": f"value like '%Class_:_{self.meta_class}%'",
                    "postFilterCondition": f"meta.Class = '{self.meta_class}'",
                    "short_table_name": ''
                }

                tech_parsedColumns = [
                    {'name': 'ChangeId', 'colType': 'string', "description": "Техническое поле"},
                    {'name': 'ChangeType', 'colType': 'string', "description": "Техническое поле"},
                    {'name': 'ChangeTimestamp', 'colType': 'string', "description": "Техническое поле"}
                ]
                self.flow[parent_table]["parsedColumns"] += tech_parsedColumns

        if table_name not in self.flow:
            self.flow[table_name] = {
                "describe_table": node.alias,
                "explodedColumns": explodedColumns,
                "tab_lvl": self.tab_lvl,
                "parsedColumns": [],
                "parent_table": '',
                "preFilterCondition": f"value like '%Class_:_{self.meta_class}%'",
                "postFilterCondition": f"meta.Class = '{self.meta_class}'",
                "short_table_name": ''
            }
        self.prepare_columns(path, explodedColumns, table_name, colType, describe_attr, node)


    def update_path(self, path:str, key:str) -> str:
        return path + f".{key}"

    def downgrade_path(self, path:str, key:str) -> str:
        return path.replace(f".{key}", "")

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