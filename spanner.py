
from google.cloud import spanner
from discover.utils.log import Logger

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
    except Exception:
        logger.error("Error in inserting in spanner, Table %s, Values %s", table_name, value_array)

def singleInsert(table_name, column_array, value):
    batchInsert(table_name, column_array, [value])


def batchInsertOrUpdate(table_name, column_array, value_array):
    logger.debug("Inserting/Updating Data, Table %s, Values %s", table_name, value_array)
    try:
        with database_discover.batch() as batch:
            batch.insert_or_update(
                table=table_name,
                columns=column_array,
                values=value_array
            )
    except Exception:
        logger.error("Error in inserting/updating in spanner, Table %s, Values %s", table_name, value_array)

def singleInsertOrUpdate(table_name, column_array, value):
    batchInsertOrUpdate(table_name, column_array, [value])


if __name__ == '__main__':
    print "initial values in table"
    results = executeQuery('select * from post')
    for row in results:
        print(row)

    # samplePostEntries={}
    # post1={'post_id':'blah1','uid':'blahuid1','createTS':int(time.time()),'eTags':'{somejson}','iTags':'{somejson}', 'priorAlpha':2.0, 'priorBeta':3.0}
    # post2={'post_id':'blah2','uid':'blahuid2','createTS':int(time.time()),'eTags':'{somejson}','iTags':'{somejson}', 'priorAlpha':2.0, 'priorBeta':3.0}
    # post3={'post_id':'blah3','uid':'blahuid3','createTS':int(time.time()),'eTags':'{somejson}','iTags':'{somejson}', 'priorAlpha':2.0, 'priorBeta':3.0}
    # samplePostEntries['post1'] = post1
    # samplePostEntries['post2'] = post2
    # samplePostEntries['post3'] = post3
    # addInitialPostEntry(samplePostEntries)

    print "inserted new values in table"
