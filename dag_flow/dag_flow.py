class Flow:
    def __init__(self, json_data:dict, loadType:str, topic:str, colsToHash:str=''):
        self.json_data = json_data
        self.loadType = loadType.lower()
        self.topic = topic.lower()
        self.colsToHash = colsToHash

    def define_target_flow(self, table_name:str) -> dict:
        target = {}
        if self.loadType == 'scd0append':
            target = {'table': table_name}
        elif self.loadType == 'scd0appendpartition':
            target = {
                'table': table_name,
                'aggregationField': 'dte',
                'partitionFields': ['dte'],
                'customPartitioning': 'Day'
            }
        else:
            target = {
                'table': table_name,
                'colsToHash': self.colsToHash
            }
        return target

    def create_flow(self) -> list:
        flows = []
        for key, values in self.json_data.items():
            # print(key, ' -> ', values)
            table_name = key
            parsedColumns = values['parsedColumns']
            explodedColumns = values['explodedColumns']
            preFilterCondition = values['preFilterCondition']
            postFilterCondition = values['postFilterCondition']
            target = self.define_target_flow(table_name)

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
