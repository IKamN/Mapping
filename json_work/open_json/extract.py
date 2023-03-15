def extract_json_values(json_file, key):
    """
    :param json_file: path to json_file
    :param key: extract json by key
    :return: json
    """
    import json
    with open(json_file, encoding='utf-8-sig') as file:
        data = json.load(file)
        return data[key]


def handle_json(json_file):
    """
    :param json_file: path to json
    :return: return json: definitions, meta_class for mapping, nodes in payload
    """
    meta_class = extract_json_values(json_file, "title").split(',')[1].split(':')[1].strip()

    properties = extract_json_values(json_file, "properties")
    payload = properties["payload"]
    items = payload["items"]

    nodes = []
    if list(items.keys()) == ['anyOf']:
        nodes +=[ref['$ref'].split('/')[-1] for ref in items['anyOf']]
    else:
        nodes += [items['$ref'].split('/')[-1]]

    definitions = extract_json_values(json_file, "definitions")

    return (definitions, meta_class, nodes)
