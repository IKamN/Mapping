from dataclasses import dataclass
from typing import Optional

@dataclass
class ParsedColumns:
    name: str
    colType: Optional[str] = "string"
    description: Optional[str] = None
    alias: Optional[str] = None

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