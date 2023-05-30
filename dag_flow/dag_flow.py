import copy

class Flow:
    def __init__(self, json_data, loadType:str, topic:str):
        self.json_data = json_data
        self.loadType = loadType
        self.topic = topic.lower()

    def define_target_flow(self, table_name:str) -> dict:
        target = {}
        if self.loadType.lower() == 'scd0append':
            target = {'table': table_name}
        elif self.loadType.lower() == 'scd0appendpartition':
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

    def create_flow(self) -> list:
        flows = []
        for values in self.json_data.flow_data.new_flow.tables:

            parsedColumns = copy.deepcopy(values.attributes.parsedColumns)
            for i in parsedColumns:
                del i.description
            for i in parsedColumns:
                if i.name == "Hdp_Processed_Dttm":
                    parsedColumns.remove(i)


            explodedColumns = values.attributes.explodedColumns
            preFilterCondition = values.preFilterCondition
            postFilterCondition = values.postFilterCondition
            target = self.define_target_flow(values.table_name)

            flows.append(
                {
                    "loadType": self.loadType,
                    "source": {
                        "schema": 'etl_schema',
                        "table": f'streaming_smart_replication_change_request_{self.topic}_default',
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

        return flows
