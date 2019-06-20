import google.auth
import yaml
from googleapiclient import discovery


def get_schema_details(projectId, datasetId, tableId):
    credentials, project = google.auth.default()
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    schema_info = bigquery.tables().get(projectId=projectId,
                                 datasetId=datasetId,
                                 tableId=tableId,
                                 fields='clustering,description,encryptionConfiguration,labels,'
                                        'schema,tableReference,timePartitioning,type,view').execute()
    schema_details = schema_info
    schema_details['datasetId'] = schema_details['tableReference'].pop('datasetId')

    resource_type = schema_details.pop('type')
    if resource_type == 'VIEW':
        schema_details.pop('schema')

    schema_details['labels']['project'] = schema_details['tableReference']['projectId'] = 'PROJECT_ID_TOKEN'
    table_name = schema_details['tableReference']['tableId']

    yaml_resource_type = 'bigquery.v2.table'

    json_for_yaml = {
        'resources': [
            {
                'name': table_name,
                'type': yaml_resource_type,
                'properties': schema_details
            }
        ]
    }
    return json_for_yaml