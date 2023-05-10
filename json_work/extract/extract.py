import json

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