
from google.cloud import spanner
from log import Logger
from google.cloud.spanner import KeySet

logger = Logger.get_logger(__file__)


# Instantiate a client.
spanner_client = spanner.Client()

# Your Cloud Spanner instance ID.
instance_id_discover = 'discoverds'

# Your Cloud Spanner database ID.
database_id_discover = 'discoverds'

# Get a Cloud Spanner instance by ID.
instance_discover = spanner_client.instance(instance_id_discover)
database_discover = instance_discover.database(database_id_discover)


def executeQuery(query):
    with database_discover.snapshot() as snapshot:
        results = snapshot.execute_sql(query)
        return results


def batchInsert(table_name, column_array, value_array):
    logger.debug("Inserting Data, Table %s, Values %s", table_name, value_array)
    try:
        with database_discover.batch() as batch:
            batch.insert(
                table=table_name,
                columns=column_array,
                values=value_array
            )
        return True
    except Exception:
        logger.error("Error in inserting in spanner, Table %s, Values %s", table_name, value_array)
        return False

def singleInsert(table_name, column_array, value):
    return batchInsert(table_name, column_array, [value])


def batchInsertOrUpdate(table_name, column_array, value_array):
    logger.debug("Inserting/Updating Data, Table %s, Values %s", table_name, value_array)
    try:
        with database_discover.batch() as batch:
            batch.insert_or_update(
                table=table_name,
                columns=column_array,
                values=value_array
            )
        return True
    except Exception:
        logger.error("Error in inserting/updating in spanner, Table %s, Values %s", table_name, value_array)
        return False

def singleInsertOrUpdate(table_name, column_array, value):
    return batchInsertOrUpdate(table_name, column_array, [value])


def singleDelete(table_name, keys_value):
    to_delete = KeySet(keys=[[keys_value]])
    logger.debug("Deleting Data, Table %s, Value %s", table_name,to_delete)
    try:
        batch.delete('PostCreator', to_delete)
        return True
    except Exception:
        logger.error("Error in deleting from spanner, Table %s, Values %s", table_name, keys_value)
        return False

def batchDelete(table_name, keys_array):
    try:
        for uid in keys_array:
            singleDelete(table_name,uid)
        return True
    except Exception:
        logger.error("Error from deleting in spanner, Table %s, Values %s", table_name, keys_value)
        return False


def get_priority_users(table_name,user_type):
    users=[]
    try:
        query="Select uid from "+table_name+" where userType="+str(user_type)
        logger.error("Query is:"+query)
        results = executeQuery(query)
        for row in results:
           users.append(row)
        return users
    except Exception:
        logger.error("Error while fetching from spanner, Table %s, user_type %s", table_name,user_type)
        return results
