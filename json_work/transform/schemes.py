from dataclasses import dataclass
from typing import Optional

@dataclass
class ParsedColumns:
    name: str
    colType: Optional[str] = "string"
    alias: Optional[str] = None
    description: Optional[str] = None

@dataclass
class TableAttributes:
    explodedColumns: list[str]
    tab_lvl: int
    parsedColumns: list[ParsedColumns]
    preFilterCondition: str
    postFilterCondition: str
    describe_table: Optional[str] = ""

@dataclass
class Table:
    table_name: str
    attributes: TableAttributes
    full_table_name: Optional[str] = None
    parent_table: Optional[str] = None

@dataclass
class Flow:
    tables: list[Table]

    def rename_table(self, old_table_name:str, new_table_name:str) -> None:
        try:
            table = next(table for table in self.tables if table.table_name == old_table_name)
            setattr(table, "table_name", new_table_name)
        except StopIteration:
            print("Rename table exception")

    def append_attr(self, curr_table:str, parent_table:str=None, full_table_name:str=None, parsedColumns:list = None) -> None:
        try:
            table = next(table for table in self.tables if table.table_name == curr_table)
            if parent_table:
                table.parent_table = parent_table
            if full_table_name:
                table.full_table_name = full_table_name.replace(".", "")
            if parsedColumns:
                parsed_objects = []
                for item in parsedColumns:
                    parsed_rows = ParsedColumns(
                        name=item["name"],
                        colType=item["colType"],
                        alias=item["alias"] if "alias" in item else None,
                        description=item["description"]
                    )
                    parsed_objects.append(parsed_rows)
                table.attributes.parsedColumns.append(parsed_rows)

        except StopIteration:
            print("Table not found")
