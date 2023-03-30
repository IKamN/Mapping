import re

tech_fields = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']

def append_to_dict(old_map, mapping_dict, node_name, tab_lvl, table_name, code_attr, describe_attr, describe_table, colType, explodedColumns):
    mapping_dict['tab_lvl'] += [tab_lvl]
    mapping_dict['table_name'] += [table_name]
    mapping_dict['short_name'] += [shorten_string(table_name)] if len(table_name) > 60 else ['']
    mapping_dict['parent_table'] += ['_'.join(table_name.split('_')[:-1]) if tab_lvl > 0 else '']
    mapping_dict['code_attr'] += [code_attr]
    mapping_dict['describe_attr'] += [describe_attr]
    mapping_dict['describe_table'] += describe_table
    mapping_dict['colType'] += [colType]
    mapping_dict['explodedColumns'] += [explodedColumns]
    mapping_dict['filter_condition'] += [node_name]
    mapping_dict['old_map'] += [code_attr + '_' + old_map]


def handle_path(explodedColumns, new_path) -> list():
    handle_path = []
    for i in range(0, len(new_path)):
        if new_path[i] == explodedColumns[-1].split('.')[-1]:
            handle_path = new_path[i:]
    return handle_path

def repeat_action(mapping_dict, tab_lvl, next_node, payload_node, path, key, value, describe_attr, description, definitions, start_table, describe_table, explodedColumns):
    tab_lvl += 1
    new_describe_attr = describe_attr + [value[f'{description}']] if f'{description}' in value else describe_attr
    # new_table = start_table + "_" + key #Short name but with duplicates
    new_describe_table = [definitions.get(next_node)[f'{description}']] if f'{description}' in definitions.get(
        next_node) else describe_table
    explodedColumns.append('.'.join(path))
    new_table = start_table + "_" + ''.join(path[1:]) # Long name but without duplicates
    listing_definition(mapping_dict, definitions, description, next_node, payload_node, path, tab_lvl, new_table, new_describe_attr, new_describe_table, explodedColumns)
    explodedColumns.pop()
    tab_lvl -= 1

def check_ref(value, path, key, describe_attr, description, explodedColumns):
        next_node = value["$ref"].split('/')[-1]
        new_path = path + [key]
        explodedPath = handle_path(explodedColumns, new_path)
        new_describe_attr = describe_attr + [value[f'{description}']] if f'{description}' in value else describe_attr
        return [next_node, explodedPath, new_describe_attr]


def shorten_string(start_table):
    tmp = start_table.split('_')[1:]
    prefix = start_table.split('_')[0]
    for i in range(0, len(tmp)-1):
        if i == 0:
            for l in range(len(tmp[i])-1, 0, -1):
                if re.search('[A-Z]', tmp[i][l]):
                    # print(tmp[i][(tmp[i].index(l) + r + 1)])
                    for r in range(3, 7):
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
                            for r in range(3, 7):
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

    # print(prefix + '_' + '_'.join(tmp), '  ', len(prefix + '_' + '_'.join(tmp)))
    return prefix + '_' + '_'.join(tmp)


def listing_definition(mapping_dict, definitions, description, node_name, payload_node, path, tab_lvl, start_table, describe_attr, describe_table, explodedColumns):
    node = definitions.get(node_name)
    properties = node.get("properties", {})

    if (start_table not in mapping_dict['table_name']) and (', '.join(explodedColumns) not in mapping_dict['explodedColumns']):
        # Add tech fileds
        for tech in tech_fields:
            append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, tech, 'Техническое поле',
                           describe_table,
                           'string' if tech != 'hdp_processed_dttm' else 'timestamp',
                           ', '.join(explodedColumns))

    # Add hash fileds
    if (tab_lvl != 0) & (explodedColumns[-1] not in mapping_dict['code_attr']):
        hash_field = explodedColumns[-1]
        array_field = ''.join(explodedColumns[-1].split('.')[1:]) + '_array'
        parent_table = '_'.join(start_table.split('_')[:-1])

        # Add in daughter
        append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, array_field,
                       describe_attr[-1] + f' (связь с {parent_table})' if len(describe_attr) > 0 else f' (связь с {parent_table})',
                       describe_table, 'hash', ', '.join(explodedColumns))

        # Add in parent
        append_to_dict(node_name, mapping_dict, payload_node, tab_lvl-1, parent_table, hash_field,
                       describe_attr[-1] + f' (связь с {start_table})' if len(describe_attr) > 0 else f' (связь с {start_table})',
                       describe_table, 'hash', ', '.join(explodedColumns[:-1]))


    if not properties:
        handlePath = handle_path(explodedColumns, path)
        append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, '_'.join(handlePath),
                       node[f'{description}'] if f'{description}' in node else '',
                       describe_table, node['type'] if 'type' in node else 'string', ', '.join(explodedColumns))


    for key, value in properties.items():
        if isinstance(value, dict):
            if "$ref" in value:
                isRefs = check_ref(value, path, key, describe_attr, description, explodedColumns)
                listing_definition(mapping_dict, definitions, description, isRefs[0], payload_node, isRefs[1], tab_lvl, start_table, isRefs[2], describe_table, explodedColumns)
            elif "anyOf" in value:
                for ref in value["anyOf"]:
                    if '$ref' in ref:
                        isRefs = check_ref(ref, path, key, describe_attr, description, explodedColumns)
                        listing_definition(mapping_dict, definitions, description, isRefs[0], payload_node, isRefs[1],tab_lvl, start_table, short_table_name, isRefs[2], describe_table, explodedColumns)
                    else:
                        handlePath = handle_path(explodedColumns, path+[key])
                        append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, '_'.join(handlePath), value[f'{description}'] if f'{description}' in value else '',
                                       describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))
            elif value.get("type") == "array":
                if 'anyOf' in value['items']:
                    for ref in value['items']['anyOf']:
                        if "$ref" in ref:
                            isRefs = check_ref(ref, path, key, describe_attr, description, explodedColumns)
                            repeat_action(mapping_dict, tab_lvl, isRefs[0], payload_node, isRefs[1], key, value, isRefs[2], description, definitions, start_table, describe_table, explodedColumns)
                        else:
                            handlePath = handle_path(explodedColumns, path+[key])
                            append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, '_'.join(handlePath),
                                           value[f'{description}'] if f'{description}' in value else '',
                                           describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))
                else:
                    if "$ref" in value["items"]:
                        isRefs = check_ref(value['items'], path, key, describe_attr, description, explodedColumns)
                        repeat_action(mapping_dict, tab_lvl, isRefs[0], payload_node, isRefs[1], key, value, isRefs[2], description, definitions, start_table, describe_table, explodedColumns)
                    else:
                        handlePath = handle_path(explodedColumns, path+[key])
                        append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, '_'.join(handlePath),
                                       value[f'{description}'] if f'{description}' in value else '',
                                       describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))

            else:
                handlePath = handle_path(explodedColumns, path+[key])
                append_to_dict(node_name, mapping_dict, payload_node, tab_lvl, start_table, '_'.join(handlePath),
                               value[f'{description}'] if f'{description}' in value else '',
                               describe_table, value['type'] if 'type' in value else 'string', ', '.join(explodedColumns))




def parsing_json(definitions, nodes, database):
    """
    :param definitions: json with definitions
    :param meta_class: meta class in messages
    :param database: database
    :return: mapping dictionary
    """

    # definitions           -- definitions      (json definitions)
    # description           -- description      (title or alias)
    # node                  -- node_name        (nodes in payload)
    # []                    -- path             (path for dags)
    # tab_lvl               -- tab_lvl          (level table)
    # start_table           -- start_table      (first table in payload)
    # []                    -- describe_attr    (describe paths)
    # [describe_table]      -- describe_table   (describe tables)
    # start_explodedColumns -- explodedColumns  (arrays in json)
    mapping_dict = {'table_name': [],
                    'short_name': [],
                    'parent_table':[],
                    'code_attr': [],
                    'tab_lvl': [],
                    'describe_attr': [],
                    'describe_table': [],
                    'comment': '',
                    'colType': [],
                    'explodedColumns': [],
                    'filter_condition': [],
                    'old_map':[]
                    }

    for node in nodes:
        tab_lvl = 0
        start_explodedColumns = ['payload']
        start_path = ['payload']
        start_table = f"{database}_" + node
        describe_table = definitions[node]['alias'] if 'alias' in definitions[node] else []
        description = 'alias' if 'alias' in definitions[node] else 'title'
        listing_definition(mapping_dict, definitions, description, node, node, start_path, tab_lvl, start_table, [], [describe_table], start_explodedColumns)

    # print({key:len(values) for key, values in mapping_dict.items()})
    return mapping_dict