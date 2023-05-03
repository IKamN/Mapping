import pprint
import re
from json_work.handle_json.mapping_data import Payload
# from json_work.handle_json.mapping_data import Parsed_rows

tech_fields = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
tmp_description = ''
orderBy = 0 # Add orderBy for sorting in dataframe excel mapping

def append_to_dict(mapping_dict, node_name, tab_lvl, table_name, code_attr, alias, describe_attr, describe_table, colType, explodedColumns):
    parsedColumns = [{'name': code_attr, 'colType': colType, 'alias': alias.lower()}] if alias != '' else [{'name': code_attr, 'colType': colType}]
    excelColumns = [{'code_attr': alias, 'describe_attr':describe_attr, 'colType': colType}] if alias != '' else [{'code_attr': code_attr, 'describe_attr':describe_attr, 'colType': colType}]

    if table_name not in mapping_dict.keys():
        mapping_dict[table_name] = {}
        mapping_dict[table_name]['tab_lvl'] = tab_lvl
        mapping_dict[table_name]['short_table_name'] = shorten_string(table_name) if len(table_name) > 60 else ''
        mapping_dict[table_name]['parent_table'] = '_'.join(table_name.split('_')[:-1]) if tab_lvl > 0 else ''
        mapping_dict[table_name]['describe_table'] = describe_table
        mapping_dict[table_name]['explodedColumns'] = [explodedColumns]
        mapping_dict[table_name]['preFilterCondition'] = node_name
        mapping_dict[table_name]['postFilterCondition'] = node_name
        mapping_dict[table_name]['incrementField'] = 'hdp_processed_dttm'
        mapping_dict[table_name]['parsedColumns'] = parsedColumns
        mapping_dict[table_name]['excelRows'] = excelColumns
    else:
        if code_attr != 'hdp_processed_dttm':
            mapping_dict[table_name]['parsedColumns'] += parsedColumns
        mapping_dict[table_name]['excelRows'] += excelColumns

    # For sorting in excel file
    # global orderBy
    # mapping_dict['orderBy'] += [orderBy]
    # orderBy += 1


def handle_path(explodedColumns, new_path) -> list():
    handle_path = []
    for i in range(0, len(new_path)):
        if new_path[i] == explodedColumns[-1].split('.')[-1]:
            handle_path = new_path[i:]
    return handle_path

def repeat_action(mapping_dict, tab_lvl, next_node, payload_node, path, key, value, describe_attr, description, definitions, start_table, describe_table, explodedColumns, alias):

    tab_lvl += 1
    new_describe_attr = describe_attr + value[f'{description}'] if f'{description}' in value else describe_attr
    new_describe_table = [definitions.get(next_node)[f'{description}']] if f'{description}' in definitions.get(next_node) else describe_table
    explodedColumns.append('.'.join(path))
    new_table = start_table + "_" + ''.join(path[1:]) # Long name but without duplicates

    if tab_lvl != 0:
        hash_field = explodedColumns[-1]
        array_field = ''.join(explodedColumns[-1].split('.')[1:]) + '_array'
        tmp_alias = array_field.replace('array', 'hash')
        parent_table = '_'.join(new_table.split('_')[:-1])

        # Add tech fields
        tech_condition = new_table not in mapping_dict.keys()
        if tech_condition:
            for tech in tech_fields:
                append_to_dict(mapping_dict, payload_node, tab_lvl, new_table, tech, '', 'Техническое поле',
                               describe_table,
                               'string' if tech != 'hdp_processed_dttm' else 'timestamp',
                               ','.join(explodedColumns))

        # Add in daughter
        append_to_dict(mapping_dict, payload_node, tab_lvl, new_table, array_field, tmp_alias,
                       new_describe_attr[-1] + f' (связь с {parent_table})' if len(new_describe_attr) > 0 else f' (связь с {parent_table})',
                       new_describe_table, 'hash', ', '.join(explodedColumns))

        # Add in parent
        append_to_dict(mapping_dict, payload_node, tab_lvl-1, parent_table, hash_field, tmp_alias,
                       new_describe_attr[-1] + f' (связь с {new_table})' if len(new_describe_attr) > 0 else f' (связь с {new_table})',
                       new_describe_table, 'hash', ', '.join(explodedColumns[:-1]))

    listing_definition(mapping_dict, payload_data)
    listing_definition(mapping_dict, definitions, description, next_node, payload_node, path, tab_lvl, new_table, new_describe_attr, new_describe_table, explodedColumns, alias)
    explodedColumns.pop()
    tab_lvl -= 1


def check_ref(value, path, key, describe_attr, description, explodedColumns, alias, flag_alias=None):
    next_node = value["$ref"].split('/')[-1]
    if flag_alias is not None:
        new_alias = alias + next_node
    else:
        new_alias = alias + key if alias != '' else key
    new_path = path + [key]
    explodedPath = handle_path(explodedColumns, new_path)
    new_describe_attr = describe_attr + value[f'{description}'] if f'{description}' in value else describe_attr
    return [next_node, explodedPath, new_describe_attr, new_alias]


def shorten_string(start_table):
    tmp = start_table.split('_')[1:]
    prefix = start_table.split('_')[0]
    for i in range(0, len(tmp)-1):
        if i == 0:
            for l in range(len(tmp[i])-1, 0, -1):
                if re.search('[A-Z]', tmp[i][l]):
                    for r in range(2, 7):
                        if tmp[i][l + r] in ['b','c','d','f','g','h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'y', 'z']:
                            tmp[i] = tmp[i][0:l+r+1]
                            break
                    break
        else:
            parts = re.findall('[A-Z][^A-Z]*', tmp[i])
            if len(parts)>1:
                for k in range(0, len(parts)):
                    for l in range(0, len(parts[k])):
                        if re.search('[A-Z]', parts[k][l]):
                            for r in range(2, 7):
                                if parts[k][r] in ['b','c','d','f','g','h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'y', 'z']:
                                    tmp[i] = tmp[i].replace(parts[k], parts[k][0:r+1])
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
                            if tmp[i][r] in ['b','c','d','f','g','h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'y', 'z']:
                                tmp[i] = tmp[i][0:r+1]
                                break
                        break
        if len(prefix + '_' + '_'.join(tmp)) > 60:
            continue
        else:
            break
    return prefix + '_' + '_'.join(tmp)


def listing_definition(mapping_dict, payload_data):

    node = payload_data.get_node()
    properties = payload_data.get_properties()

    # Add tech fields
    tech_condition = start_table not in mapping_dict.keys()
    if tech_condition:
        for tech in tech_fields:
            append_to_dict(mapping_dict, payload_node, tab_lvl, start_table, tech, '', 'Техническое поле',
                           describe_table,
                           'string' if tech != 'hdp_processed_dttm' else 'timestamp',
                           ','.join(explodedColumns))


    if not properties:
        handlePath = handle_path(explodedColumns, path)
        tmp_alias = handlePath[1:] if alias == '' else alias
        append_to_dict(mapping_dict, payload_node, tab_lvl, start_table, '.'.join(handlePath), tmp_alias,
                       node[f'{description}'] if f'{description}' in node else '',
                       describe_table, node['type'] if 'type' in node else 'string', ', '.join(explodedColumns))


    for key, value in properties.items():
        if isinstance(value, dict):
            if "$ref" in value:
                isRefs = check_ref(value, path, key, describe_attr, description, explodedColumns, alias)
                listing_definition(mapping_dict, payload_data)
                listing_definition(mapping_dict, definitions, description, isRefs[0], payload_node, isRefs[1], tab_lvl, start_table, isRefs[2], describe_table, explodedColumns, isRefs[3])
            elif "anyOf" in value:
                for ref in value["anyOf"]:
                    if '$ref' in ref:
                        isRefs = check_ref(ref, path, key, describe_attr, description, explodedColumns, alias, '1')
                        listing_definition(mapping_dict, payload_data)
                        listing_definition(mapping_dict, definitions, description, isRefs[0], payload_node, isRefs[1], tab_lvl, start_table, isRefs[2], describe_table, explodedColumns, isRefs[3])
                    else:
                        handlePath = handle_path(explodedColumns, path+[key])
                        append_to_dict(mapping_dict, payload_node, tab_lvl, start_table, '.'.join(handlePath), alias, value[f'{description}'] if f'{description}' in value else '',
                                       describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))
            elif value.get("type") == "array":
                if 'anyOf' in value['items']:
                    for ref in value['items']['anyOf']:
                        if "$ref" in ref:
                            isRefs = check_ref(ref, path, key, describe_attr, description, explodedColumns, alias, '1')
                            repeat_action(mapping_dict, tab_lvl, isRefs[0], payload_node, isRefs[1], key, value, isRefs[2], description, definitions, start_table, describe_table, explodedColumns, isRefs[3])
                        else:
                            handlePath = handle_path(explodedColumns, path+[key])
                            append_to_dict(mapping_dict, payload_node, tab_lvl, start_table, '.'.join(handlePath), alias,
                                           value[f'{description}'] if f'{description}' in value else '',
                                           describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))
                else:
                    if "$ref" in value["items"]:
                        isRefs = check_ref(value['items'], path, key, describe_attr, description, explodedColumns, alias)
                        repeat_action(mapping_dict, tab_lvl, isRefs[0], payload_node, isRefs[1], key, value, isRefs[2], description, definitions, start_table, describe_table, explodedColumns, alias)
                    else:
                        handlePath = handle_path(explodedColumns, path+[key])
                        append_to_dict(mapping_dict, payload_node, tab_lvl, start_table, '.'.join(handlePath), alias,
                                       value[f'{description}'] if f'{description}' in value else '',
                                       describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))

            else:
                handlePath = handle_path(explodedColumns, path+[key])
                tmp_alias = key if alias == '' else  alias + key
                append_to_dict(mapping_dict, payload_node, tab_lvl, start_table, '.'.join(handlePath), tmp_alias,
                               value[f'{description}'] if f'{description}' in value else '',
                               describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))




def parsing_json(definitions, nodes, database):
    """
    :param definitions: json with definitions
    :param nodes: refs in payload
    :param database: database
    :return: mapping dictionary
    """

    mapping_dict = {}
    for node in nodes:
        payload_data = Payload(definitions, node, database)
        listing_definition(mapping_dict, payload_data)

    # print({key:len(values) for key, values in mapping_dict.items()})
    # pprint.pprint(mapping_dict)
    return mapping_dict