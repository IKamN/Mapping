from dataclasses import dataclass
from typing import Optional


@dataclass
class SourceTarget:
    index: Optional[str] = "#"
    object: Optional[str] = "Реплика"


@dataclass
class SourceHeaders:
    base: str
    _class: str
    _class_name: str
    tag_json: str
    tag_descr: str
    data_type: str
    length: Optional[str] = None
    PK: Optional[str] = None
    FK: Optional[str] = None
    NotNull: Optional[str] = None


@dataclass
class TargetHeaders:
    schema: str
    table: str
    parent_table: str
    descr_table: str
    code_attr: str
    descr_code: str
    comment: str
    data_type: str
    base: Optional[str] = "1642_19 Озеро данных"
    length: Optional[str] = None
    pk: Optional[str] = None
    fk: Optional[str] = None
    notnull: Optional[str] = None
    rejectable: Optional[str] = None
    trace: Optional[str] = None


@dataclass
class s2t:
    source_target: SourceTarget
    source: SourceHeaders
    target: TargetHeaders