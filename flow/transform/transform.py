import typing
from flow.transform.model import Attributes, Node, Transform

if typing.TYPE_CHECKING:
    from flow.flow import Flow

def setup_transform(flow: "Flow", database: str) -> None:

    data: Transform = Transform(flow.extract.meta_class)

    def listing_definitions(ref, table, path, explodedColumns, describe_attr: str) -> None:
        node: Node = Node(flow.extract.definitions.get(ref))
        table: str = table.lower().replace(".", "_")

        if not any(t.table_name == table for t in data.new_flow.tables):
            data.append_table(table, node.alias, explodedColumns, flow.extract.anyOf)
            if data.tab_lvl != 0:
                data.append_hash(table, explodedColumns)

        if hasattr(node, "properties"):
            for key, value in node.properties.items():
                new_path: str = data.update_path(path, key)
                attr_key: Attributes = Attributes(value)
                cnt_refs: int = len(attr_key.refs)
                if cnt_refs != 0:
                    for ref in attr_key.refs:
                        if (hasattr(attr_key, "type")) and (attr_key.type == 'array'):
                            updated = data.next_array(new_path, explodedColumns, table)
                            array_table = updated.get("table")
                            array_explodedColumns = updated.get("explodedColumns")
                            array_path = updated.get("path")
                            listing_definitions(ref, array_table, array_path, array_explodedColumns, attr_key.alias)
                            data.tab_lvl -= 1
                        else:
                            listing_definitions(ref, table, new_path, explodedColumns, attr_key.alias)
                else:
                    data.append_columns(new_path, table, attr_key.type, attr_key.alias, flow.extract.anyOf)
        else:
            data.append_columns(path, table, "string", describe_attr, flow.extract.anyOf)

    for start_table in flow.extract.payload_refs:
        start_path: str = "payload"
        ref: str = start_table
        start_table: str = f"{database}_{start_table}"
        explodedColumns: list[str] = ["payload"]
        describe_attr: str = ""
        listing_definitions(ref, start_table, start_path, explodedColumns, describe_attr)

    flow.transform: Transform = data