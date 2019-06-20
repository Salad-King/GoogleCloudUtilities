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

    schema_details.pop('type')
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


if __name__ == "__main__":
    table_json = get_schema_details("prj-gousenaib-dlak-res01", "consumption_layer_sensitive", "bv_merchant_pii_information")
    with open(r'C:\Users\mohammedt\Desktop\test.yaml', 'w+') as stream:
        yaml.safe_dump(table_json, stream, allow_unicode=True,
                       default_flow_style=False)
