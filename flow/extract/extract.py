from dataclasses import dataclass

@dataclass
class Extract:
    meta_class: str
    payload_refs: list[str]
    definitions: dict
    anyOf: bool


def __get_refs(items: dict) -> list[str]:
    """
    Get refs in payload
    """
    refs: list = []
    if list(items.keys()) == ["anyOf"]:
        refs += [ref.get("$ref").split("/")[-1] for ref in items.get("anyOf")]
    else:
        refs += [items.get("$ref").split("/")[-1]]
    return refs

def read_json(flow, json_file:str):
    """
    Read json_file. Get meta, payload, definitions
    """
    import json

    with open(json_file, encoding="utf-8-sig") as file:
        data: dict = json.load(file)
        meta_class: str = data.get("title").split(",")[1].split(":")[1].strip()
        payload_refs: list[str] = __get_refs(data.get("properties").get("payload").get("items"))
        definitions: dict = data.get("definitions")
        anyOf: bool = True if len(payload_refs) > 1 else False

        flow.extract = Extract(
            meta_class = meta_class,
            payload_refs = payload_refs,
            definitions = definitions,
            anyOf = anyOf
        )




