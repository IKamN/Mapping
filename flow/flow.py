import copy
import pprint
from typing import Optional
import os

from flow.S2Tmapping.S2Tmapping import create_mapping
from flow.extract.extract import Extract, read_json
from flow.naming.naming import setup_naming
from flow.transform.transform import Transform, setup_transform

class Flow:
    def __init__(self):
        self.flow: Optional[list] = []
        self.extract: Optional[Extract]
        self.transform: Optional[Transform]

    def __define_target_flow(self, table_name:str, loadType: str) -> dict:
        if loadType.lower() == 'scd0append':
            target = {'table': table_name}
        elif loadType.lower() == 'scd0appendpartition':
            target = {
                'table': table_name,
                'aggregationField': 'dte',
                'partitionFields': ['dte'],
                'customPartitioning': 'Day',
                'updateAllowed': True
            }
        else:
            target = {
                'table': table_name
            }
        return target

    def create_flow(self, loadType: str, topic: str) -> None:
        for values in self.transform.new_flow.tables:
            parsedColumns = copy.deepcopy(values.attributes.parsedColumns)
            for i in parsedColumns:
                del i.description
                del i.comment
            for i in parsedColumns:
                if i.name == "hdp_processed_dttm":
                    parsedColumns.remove(i)
                if i.colType not in ["timestamp", "hash"]:
                    i.colType = "string"

            explodedColumns = values.attributes.explodedColumns
            preFilterCondition = values.preFilterCondition
            postFilterCondition = values.postFilterCondition
            target = self.__define_target_flow(values.table_name, loadType)

            self.flow.append(
                {
                    "loadType": loadType,
                    "source": {
                        "schema": 'etl_schema',
                        "table": f'streaming_smart_replication_change_request_{topic}_default',
                        "columnsWithJson": ["value"],
                        "explodedColumns": explodedColumns,
                        "parsedColumns": parsedColumns,
                        "preFilterCondition": preFilterCondition,
                        "postFilterCondition": postFilterCondition,
                        "incrementField": "hdp_processed_dttm"
                    },
                    'target': target,
                    'addInfo': {'orderField': 'changeTimestamp'}
                })


def setup_flow(app: "App"):

    new_flow: Flow = Flow()
    app.flow = new_flow
    for filename in os.listdir(app.config.file_dir):
        if filename.endswith('json'):
            json_file:str = os.path.join(app.config.file_dir, filename)

            # Get meta, payload, definitions
            read_json(new_flow, json_file)

            # Parsing json, return FlowProcess object
            setup_transform(new_flow, app.config.database)

            # Change table names, alias in parsedColumns
            setup_naming(new_flow)

            # Append paths
            new_flow.create_flow(loadType=app.config.loadType, topic=app.config.topic)

            # Create and save S2T mapping
            create_mapping(new_flow, json_file, app.config.subo_name, app.config.mapping_version, app.config.database)