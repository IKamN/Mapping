import json
from pprint import pprint

class Extract:
    def __init__(self, json_file:json):
        self.json_file = json_file

    def open_json(self):
        """
        Open json_file and transform, return new dict
        """

        def get_refs(items):
            """
            Get refs in payload
            """
            refs = []
            if list(items.keys()) == ['anyOf']:
                refs += [ref['$ref'].split('/')[-1] for ref in items['anyOf']]
            else:
                refs += [items['$ref'].split('/')[-1]]
            return refs

        with open(self.json_file, encoding='utf-8-sig') as file:
            data = json.load(file)
            meta_class = data['title'].split(',')[1].split(':')[1].strip()
            payload_refs = get_refs(data['properties']['payload']["items"])
            definitions = data['definitions']
            new_json = {
                'meta': meta_class,
                'payload': payload_refs,
                'definitions': definitions
            }
        return new_json


class Json_data(Extract):
    def iterate_refs(self):
        json_file = self.open_json()
        payload_refs = json_file['payload']
        definitions = json_file['definitions']
        meta_class = json_file['meta']

        flow = Flow(meta_class)

        def listing_definitions(ref):
            node = Node(definitions[ref])
            if hasattr(node, 'properties'):
                for key, value in node.properties.items():
                    print(key, '-->', value)
                    attr_tag = Attributes(value)
                    if len(attr_tag.refs) != 0:
                        for ref in attr_tag.refs:
                            if (hasattr(attr_tag, "type")) and (attr_tag.type == 'array'):
                                flow.append_table(key, node)
                            listing_definitions(ref)
                            flow.back(key)
                    else:
                        pass
        for ref in payload_refs:
            flow.append_parent(ref, definitions)
            listing_definitions(ref)
            flow.print_flow()



class Attributes:
    def __init__(self, properties_dict: dict):
        for key, value in properties_dict.items():
            if key == "title":
                setattr(self, "alias", value)
            elif key == "$ref":
                setattr(self, "ref", value)
            else:
                setattr(self, key, value)
            self.refs = []

        if (hasattr(self, "type")) and (self.type == "array"):
            if 'anyOf' in self.items:
                self.refs += [i['$ref'].split('/')[-1] for i in self.items['anyOf']]
            elif '$ref' in self.items:
                self.refs.append(self.items['$ref'].split('/')[-1])
            else:
                self.refs.append('smthMissed..')
        elif hasattr(self, 'anyOf'):
            self.refs += [i['ref'].split('/')[-1] for i in self.anyOf]
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

class Path:
    def __init__(self):

class Flow:
    from typing import Optional

    def __init__(self, meta_class):
        self.meta_class = meta_class
        self.flow: dict = {}
        self.tab_lvl = 0
        self.table_name = ''

    def append_parent(self, ref:str, definitions:dict):
        node = Node(definitions[ref])
        self.table_name = ref # Reformat table_name
        if ref not in self.flow:
            self.flow[ref] = {
                "describe_table": node.alias,
                "explodedColumns": ["payload"],
                "tab_lvl": self.tab_lvl,
                "parent_table": '',
                "parsedColumns": [],
                "preFilterCondition": self.meta_class,
                "postFilterCondition": self.meta_class
            }
    def append_table(self, key: str, node: Optional[Node] = None) -> None:
        self.table_name = self.table_name + '_' + key
        self.tab_lvl += 1
        if self.table_name not in self.flow:
            meta_class = self.meta_class
            self.flow[self.table_name] = {
                "describe_table": node.alias,
                "explodedColumns": ["payload"],
                "tab_lvl": self.tab_lvl,
                "parent_table": '',
                "parsedColumns": [],
                "preFilterCondition": meta_class,
                "postFilterCondition": meta_class
            }
    def back(self, key):
        self.tab_lvl -= 1
        self.table_name =  self.table_name.replace('_' + key, '')

    def print_flow(self):
        pprint(self.flow)


test = Json_data('/media/kini/B0E4EF45E4EF0C82/PythonFolder/parsing/VTB/json_excel/SalesPointDirectory_v.1.04.json')
test.iterate_refs()