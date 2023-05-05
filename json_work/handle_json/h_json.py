import pprint
import re
from json_work.json_data.json_data import Payload
# from json_work.handle_json.mapping_data import Parsed_rows

tech_fields = ['changeid', 'changetype', 'changetimestamp', 'hdp_processed_dttm']
tmp_description = ''
orderBy = 0 # Add orderBy for sorting in dataframe excel mapping

def append_to_dict(mapping_dict, payload_data):
    parsedColumns = [{'name': payload_data.path, 'colType': payload_data.colType, 'alias': payload_data.alias.lower()}] if payload_data.alias != '' else [{'name': payload_data.path, 'colType': payload_data.colType}]
    excelColumns = [{'code_attr': payload_data.alias, 'describe_attr': payload_data.describe_attr, 'colType': payload_data.colType}] if payload_data.alias != '' else [{'code_attr': payload_data.code_attr, 'describe_attr': payload_data.describe_attr, 'colType': payload_data.colType}]

    if payload_data.table_name not in mapping_dict.keys():
        mapping_dict[payload_data.table_name] = {}
        mapping_dict[payload_data.table_name]['tab_lvl'] = payload_data.tab_lvl
        mapping_dict[payload_data.table_name]['short_table_name'] = shorten_string(payload_data.table_name) if len(payload_data.table_name) > 60 else ''
        mapping_dict[payload_data.table_name]['parent_table'] = '_'.join(payload_data.table_name.split('_')[:-1]) if payload_data.tab_lvl > 0 else ''
        mapping_dict[payload_data.table_name]['describe_table'] = payload_data.describe_table
        mapping_dict[payload_data.table_name]['explodedColumns'] = [payload_data.explodedColumns]
        mapping_dict[payload_data.table_name]['preFilterCondition'] = payload_data.node
        mapping_dict[payload_data.table_name]['postFilterCondition'] = payload_data.node
        mapping_dict[payload_data.table_name]['incrementField'] = 'hdp_processed_dttm'
        mapping_dict[payload_data.table_name]['parsedColumns'] = parsedColumns
        mapping_dict[payload_data.table_name]['excelRows'] = excelColumns
    else:
        if payload_data.path != 'hdp_processed_dttm':
            mapping_dict[payload_data.table_name]['parsedColumns'] += parsedColumns
        mapping_dict[payload_data.table_name]['excelRows'] += excelColumns

    # For sorting in excel file
    # global orderBy
    # mapping_dict['orderBy'] += [orderBy]
    # orderBy += 1


def repeat_action(mapping_dict, payload_data, value):

    payload_data.prepare_repeat(value)

    if payload_data.tab_lvl != 0:
        # Add tech fields
        tech_condition = payload_data.table_name not in mapping_dict.keys()
        if tech_condition:
            for tech in tech_fields:
                append_to_dict(mapping_dict, payload_data.append_to_dict(tech))

        # Add in daughter
        append_to_dict(mapping_dict, payload_data.append_to_dict('daugh'))

        # Add in parent
        append_to_dict(mapping_dict, payload_data.append_to_dict('parent'))


    listing_definition(mapping_dict, payload_data)
    payload_data.explodedColumns.pop()
    payload_data.tab_lvl -= 1


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
    tech_condition = payload_data.table_name not in mapping_dict.keys()
    if tech_condition:
        for tech in tech_fields:
            append_to_dict(mapping_dict, payload_data.append_to_dict(tech))

    if not properties:
        handlePath = payload_data.handle_path(payload_data.explodedColumns, payload_data.path)
        # tmp_alias = handlePath[1:] if alias == '' else alias
        append_to_dict(mapping_dict, payload_data.append_to_dict())


    for key, value in properties.items():
        if isinstance(value, dict):
            if "$ref" in value:
                payload_data.check_refs(key, value)
                listing_definition(mapping_dict, payload_data)
            elif "anyOf" in value:
                for ref in value["anyOf"]:
                    if '$ref' in ref:
                        payload_data.check_refs(key, ref, '1')
                        listing_definition(mapping_dict, payload_data)
                    else:
                        append_to_dict(mapping_dict, payload_data)
            elif value.get("type") == "array":
                if 'anyOf' in value['items']:
                    for ref in value['items']['anyOf']:
                        if "$ref" in ref:
                            payload_data.check_refs(key, ref, '1')
                            repeat_action(mapping_dict, payload_data)
                        else:
                            append_to_dict(mapping_dict, payload_data)
                else:
                    if "$ref" in value["items"]:
                        payload_data.check_refs(key, value['items'])
                        repeat_action(mapping_dict, payload_data)
                    else:
                        append_to_dict(mapping_dict, payload_data)

            else:
                append_to_dict(mapping_dict, payload_data)




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
    pprint.pprint(mapping_dict)
    return mapping_dict