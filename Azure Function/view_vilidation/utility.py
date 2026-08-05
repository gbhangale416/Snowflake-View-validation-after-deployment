import os
import json
import logging
import snowflake.connector
from azure.storage.blob import BlobClient

# Global environment parameters expected by utility functions
SnowflakeServiceUser = os.environ.get("SnowflakeServiceUser")
SnowflakeServicePassword = os.environ.get("SnowflakeServicePassword")
SnowflakeServiceWarehouse = os.environ.get("SnowflakeServiceWarehouse")
authentication_type = os.environ.get("authentication_type", "False")


def get_metameta_dict(db_name: str, file_name="metameta", adf_type='edw') -> dict:
    """Downloads metameta JSON blob from Azure Blob Storage."""
    connection_string = os.environ['AzureBlobStorageConnectionString']
    if adf_type and adf_type.lower() == 'edi':
        connection_string = os.environ['AzureBlobStorageEDIConnectionString']
        
    metameta_blob_client = BlobClient.from_connection_string(
        connection_string,
        container_name="metadata",
        blob_name=f"{db_name}/{db_name}_{file_name}.json"
    )

    metameta_ssdl = metameta_blob_client.download_blob()
    metameta_blob_text = metameta_ssdl.content_as_text()
    metameta_dict = json.loads(metameta_blob_text)

    return metameta_dict


def find_entity_meta_meta(meta, source_entity_name):
    """Finds a specific entity configuration inside metadata JSON."""
    if 'entities' in meta:
        entities = meta['entities']
    else:
        entities = meta

    for ent in entities:
        if ent.get('source_entity', '').lower() == source_entity_name.lower():
            return ent
    return None


def get_entity_key_value(key, source_entity, meta):
    """Retrieves a config key value from entity level or fallback to top-level meta."""
    if source_entity and key in source_entity:
        return source_entity[key]
    elif meta and key in meta:
        return meta[key]
    return None


def get_snowflake_connection():
    """Establishes and returns Snowflake cursor and connection object."""
    try:
        snowflakeConnection = {"account": 'test.west-us.privatelink'}
        snowflakeConnection["timeout"] = 180

        if SnowflakeServiceUser:
            snowflakeConnection["user"] = SnowflakeServiceUser

        if authentication_type and authentication_type == 'True':
            snowflakeConnection["authenticator"] = 'externalbrowser'
        else:
            if SnowflakeServicePassword:
                snowflakeConnection["password"] = SnowflakeServicePassword

        snowflakeConnection["warehouse"] = SnowflakeServiceWarehouse
        ctx = snowflake.connector.connect(**snowflakeConnection)
        cs = ctx.cursor()
        return cs, ctx
    except Exception as error:
        logging.error(f"get_snowflake_connection() - Unexpected error: {error}")
        raise
