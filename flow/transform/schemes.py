from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedColumns:
    name: str
    colType: Optional[str] = "string"
    alias: Optional[str] = None
    description: Optional[str] = None
    comment: Optional[str] = ""

    def __repr__(self):
        attributes = ", ".join(f"'{attr}':'{value}'" for attr, value in self.__dict__.items())
        return "{"+f"{attributes}"+"}"

@dataclass
class TableAttributes:
    explodedColumns: list[str]
    parsedColumns: list[ParsedColumns]

@dataclass
class Table:
    table_name: str
    attributes: TableAttributes
    tab_lvl: int
    preFilterCondition: str
    postFilterCondition: str
    describe_table: Optional[str] = ""
    full_table_name: Optional[str] = None
    parent_table: Optional[str] = None

@dataclass
class TableData:
    tables: list[Table]

    def find_table(self, curr_table:str):
        try:
            table = next(table for table in self.tables if table.table_name == curr_table)
            return table
        except StopIteration:
            print("Rename table exception")

    def rename_table(self, old_table_name:str, new_table_name:str) -> None:
        try:
            table = next(table for table in self.tables if table.table_name == old_table_name)
            setattr(table, "table_name", new_table_name)
        except StopIteration:
            print("Rename table exception")

    def append_attr(self, curr_table:str, parent_table:str=None, full_table_name:str=None, parsedColumns:dict = None, flag:str =None) -> None:
        try:
            table = next(table for table in self.tables if table.table_name == curr_table)
            if parent_table:
                table.parent_table = parent_table.replace(".", "")
            if full_table_name:
                table.full_table_name = full_table_name.replace(".", "")
            if parsedColumns:
                _names = [i.name for i in table.attributes.parsedColumns]
                if parsedColumns["name"] not in _names:
                    parsed_rows = ParsedColumns(
                        name=parsedColumns["name"],
                        colType=parsedColumns["colType"],
                        alias=parsedColumns["alias"] if "alias" in parsedColumns else None,
                        description=parsedColumns["description"],
                        comment=parsedColumns["comment"]
                    )
                    if flag:
                        table.attributes.parsedColumns.insert(4, parsed_rows)
                    else:
                        table.attributes.parsedColumns.append(parsed_rows)


        except StopIteration:
            print("Table not found")
