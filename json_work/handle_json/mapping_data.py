class Payload:
    def __init__(self,
                 definitions:dict,
                 node:str,
                 database:str,
                 ):
        self.definitions = definitions # payload json
        self.node = node # refs in payload
        self.database = database # db code

        if 'title' in definitions[node]:
            self.description_code = 'title'
        elif 'alias' in definitions[node]:
            self.description_code = 'alias'
        else:
            self.description_code = ''

        self.explodedColumns = ['payload']
        self.path = 'payload'
        self.tab_lvl = 0
        self.table_name = f'{self.database}' + self.node

        try:
            self.describe_table = self.definitions[self.node][self.description_code]
        except:
            self.describe_table = ''

    def get_node(self):
        return self.definitions[self.node]

    def get_properties(self):
        try:
            node = self.get_node()
            return node['properties']
        except:
            return {}


class Parsed_rows:
    def __init__(self,
                 payload_node:str,
                 tab_lvl:int,
                 table_name:str,
                 short_table_name:str,
                 parent_table:str,
                 describe_table:str,
                 explodedColumns:str,
                 preFilterCondition:str,
                 postFilterCondition:str,
                 incrementField:str,
                 code_attr:str,
                 colType:str,
                 alias:str):
        self.payload_node = payload_node
        self.tab_lvl = tab_lvl
        self.table_name = table_name
        self.short_table_name = short_table_name
        self.parent_table = parent_table
        self.describe_table = describe_table
        self.explodedColumns = explodedColumns
        self.preFilterCondition = preFilterCondition
        self.postFilterCondition = postFilterCondition
        self.code_attr = code_attr
        self.colType = colType
        self.alias = alias
        self.incrementField = incrementField



