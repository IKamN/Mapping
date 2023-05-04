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
        self.describe_attr = ''

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

    def handle_path(self, explodedColumns, new_path) -> list():
        handle_path = []
        for i in range(0, len(new_path)):
            if new_path[i] == explodedColumns[-1].split('.')[-1]:
                handle_path = new_path[i:]
        return handle_path

    def check_refs(self, key, value, flag_alias=None):
        next_node = value["$ref"].split('/')[-1]
        if flag_alias is not None:
            new_alias = self.alias + next_node
        else:
            new_alias = self.alias + key if self.alias != '' else key
        new_path = self.path + [key]
        explodedPath = self.handle_path(self.explodedColumns, new_path)
        new_describe_attr = self.describe_attr + value[f'{self.description}'] if f'{self.description}' in value else self.describe_attr

        self.node = next_node
        self.path = explodedPath
        self.describe_attr = new_describe_attr
        self.alias = new_alias

    def prepare_repeat(self, value):
        self.tab_lvl += 1
        self.describe_attr = self.describe_attr + value[f'{self.description_code}'] \
            if f'{self.description_code}' in value else self.describe_attr
        self.describe_table = [self.get_node()[f'{self.description_code}']] \
            if f'{self.description_code}' in self.get_node() else self.describe_table
        self.explodedColumns = self.explodedColumns.append('.'.join(self.path))
        self.table_name = self.table_name + "_" + ''.join(self.path[1:])

    def append_to_dict(self, flag=None):
        tech_fields = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
        if flag in tech_fields:
            self.path = flag
            self.alias = ''
            self.describe_attr = 'Техническое поле'
            self.colType = 'string' if self.path != 'hdp_processed_dttm' else 'timestamp'
        elif flag == 'daugh':
            parent_table = '_'.join(self.table_name.split('_')[:-1])
            array_field = ''.join(self.explodedColumns[-1].split('.')[1:]) + '_array'
            self.path = array_field
            self.alias =  self.path.replace('array', 'hash')
            self.describe_attr = self.describe_attr[-1] + f' (связь с {parent_table})' if len(self.describe_attr) > 0 else f' (связь с {parent_table})'
            self.colType = 'hash'
            self.explodedColumns = ', '.join(self.explodedColumns)
        elif flag == 'parent':
            parent_table = '_'.join(self.table_name.split('_')[:-1])
            hash_field = self.explodedColumns[-1]
            self.path = hash_field
            self.alias =  self.path.replace('array', 'hash')
            self.describe_attr = self.describe_attr[-1] + f' (связь с {self.table_name})' if len(self.describe_attr) > 0 else f' (связь с {self.table_name})'
            self.colType = 'hash'
            self.explodedColumns = ', '.join(self.explodedColumns)
        else:
            self.path = self.handle_path(self.explodedColumns, self.path)
            self.describe_attr = self.node[f'{self.description_code}'] if f'{self.description_code}' in self.node else ''
            self.colType = self.node['type'] if 'type' in self.node else 'string'
            self.explodedColumns = ', '.join(self.explodedColumns)


        self.explodedColumns = ', '.join(self.explodedColumns)










# class Parsed_rows:
#     def __init__(self,
#                  payload_node:str,
#                  tab_lvl:int,
#                  table_name:str,
#                  short_table_name:str,
#                  parent_table:str,
#                  describe_table:str,
#                  explodedColumns:str,
#                  preFilterCondition:str,
#                  postFilterCondition:str,
#                  incrementField:str,
#                  code_attr:str,
#                  colType:str,
#                  alias:str):
#         self.payload_node = payload_node
#         self.tab_lvl = tab_lvl
#         self.table_name = table_name
#         self.short_table_name = short_table_name
#         self.parent_table = parent_table
#         self.describe_table = describe_table
#         self.explodedColumns = explodedColumns
#         self.preFilterCondition = preFilterCondition
#         self.postFilterCondition = postFilterCondition
#         self.code_attr = code_attr
#         self.colType = colType
#         self.alias = alias
#         self.incrementField = incrementField
#
#
#
