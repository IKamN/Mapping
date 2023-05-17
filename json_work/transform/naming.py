import pprint
import re

class NamingPrepare:
    def __init__(self, flow_data):
        self.flow_data = flow_data
        self.sorted_data = sorted([table.table_name for table in flow_data.new_flow.tables], key=lambda x: len(x))
        self.new_data:dict = {}

        def cut_string(table_name: str) -> str:
            split_string = table_name.split('_')
            for j in range(2, len(split_string)):
                if any(c.isupper() for c in split_string[j]):
                    new_substring = ''.join([s for s in split_string[j] if s.isupper()])
                    split_string[j] = new_substring
                new_string = '_'.join(split_string)
                if len(new_string) <= 60:
                    return new_string

        def shorten_table(old_table:str) -> str:
            elements = old_table.split("_")
            if len(elements) <= 2:
                if len(old_table) > 60:
                    self.flow_data.new_flow.append_attr(old_table, full_table_name=old_table)
                    return cut_string(old_table).replace(".", "")
                return old_table
            i = 0
            while i < len(elements) - 1:
                if "." in elements[i + 1]:
                    new_element = re.sub(r'^[^.]+\.', '', elements[i + 1])
                    temp_string = "_".join(elements[:i + 1] + [new_element] + elements[i + 2:])
                    if temp_string in self.sorted_data:
                        break
                    elements = temp_string.split("_")
                else:
                    i += 1
            if len("_".join(elements).replace(".", "")) > 60:
                self.flow_data.new_flow.append_attr(old_table, full_table_name=old_table)
                return cut_string(old_table).replace(".", "")
            return "_".join(elements).replace(".", "")

        # print(self.sorted_data)
        for index in range(0, len(self.sorted_data)):
            old_table_name = self.sorted_data[index]
            new_table_name = shorten_table(self.sorted_data[index])
            self.flow_data.new_flow.rename_table(old_table_name, new_table_name)
            self.sorted_data[index] = new_table_name
        print([(table.table_name, table.parent_table) for table in self.flow_data.new_flow.tables])



class Naming:
    def __init__(self, json_data):
        self.json_data = json_data
        self.sorted_data:list = sorted(json_data.flow.keys(), key=len)
        self.new_data:dict = {}
        self.name_flag: bool = False
        self.new_sort = sorted([table.table_name for table in json_data.new_flow.tables], key = lambda x : len(x))

        def shorten_table(old_table:str) -> str:
            elements = old_table.split("_")
            # print(old_table)
            if len(elements) <= 2:
                if len(old_table) > 60:
                    self.name_flag = True
                    self.json_data.flow[old_table]["full_table_name"] = old_table.replace(".", "")
                    return self.cut_string(old_table).replace(".", "")
                return old_table
            i = 0
            while i < len(elements) - 1:
                if "." in elements[i + 1]:
                    new_element = re.sub(r'^[^.]+\.', '', elements[i + 1])
                    temp_string = "_".join(elements[:i + 1] + [new_element] + elements[i + 2:])
                    if temp_string in self.sorted_data:
                        break
                    elements = temp_string.split("_")
                else:
                    i += 1
            if len("_".join(elements).replace(".", "")) > 60:
                self.name_flag = True
                self.json_data.flow[old_table]["full_table_name"] = old_table.replace(".", "")
                return self.cut_string(old_table).replace(".", "")
            return "_".join(elements).replace(".", "")

        def shorten_alias(table_name:str) -> str:

            def get_alias(table_name: str) -> list:
                alias_lst = [i["alias"] for i in self.json_data.flow[table_name]["parsedColumns"][4:]]
                sorted_alias_lst = sorted(alias_lst, key=lambda x: len(x.split(".")))
                return sorted_alias_lst

            alias_lst = get_alias(table_name)

            def find_alias(old_alias:str, new_alias:str)->None:
                for item in self.json_data.flow[table_name]["parsedColumns"][4:]:
                    if item["alias"] == old_alias:
                        item["alias"] = new_alias

                for index in range(0, len(alias_lst)):
                    if alias_lst[index] == old_alias:
                        alias_lst[index] = new_alias


            def rename_alias(old_alias:str) -> str:
                alias = old_alias
                result_string = old_alias
                while "." in result_string:
                    if ("hash" in old_alias) and len(result_string.split(".")) == 2:
                        alias = result_string.replace(".", "_")
                        return alias

                    if old_alias.replace(".", "_") in alias_lst:
                        alias = re.sub(r'\.([^\.]+)$', r'\g<0>\g<1>', old_alias).replace(".", "_")
                        return alias

                    result_string = re.sub(r'^[^.]+\.', '', result_string)

                    if result_string.replace(".", "_") in alias_lst:
                        return alias

                    if result_string.replace(".", "_") not in alias_lst:
                        alias = result_string.replace(".", "_")
                return alias

            for alias in alias_lst:
                new_alias = rename_alias(alias)
                find_alias(alias, new_alias)

        # print(self.new_sort)
        for index in range(0, len(self.sorted_data)):
            old_table_name = self.sorted_data[index]
            shorten_alias(old_table_name)
            new_table_name = shorten_table(self.sorted_data[index])
            self.new_data[new_table_name] = json_data.flow.pop(old_table_name)
            self.sorted_data[index] = new_table_name

    def cut_string(self, table_name:str) -> str:
        split_string = table_name.split('_')
        for j in range(2, len(split_string)):
            if any(c.isupper() for c in split_string[j]):
                new_substring = ''.join([s for s in split_string[j] if s.isupper()])
                split_string[j] = new_substring
            new_string = '_'.join(split_string)
            if len(new_string) <= 60:
                return new_string
