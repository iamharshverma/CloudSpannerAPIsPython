import PySQLPool
from discover.utils.log import Logger
from discover.core.connect import mysql_manager
from discover.system_config import Config
import MySQLdb


logger = Logger.get_logger(__file__)


class QueryExecutor(object):
    # Execute and provides affected row counts
    @classmethod
    def execute_with_commit(cls, conn, query_string, commit_on_end=True):
        try:
            query = PySQLPool.getNewQuery(conn, commitOnEnd=True)
            query.Query(query_string)
        except Exception as e:
            logger.error("failed while executing query due to" + e.message)
            return e.message
        if commit_on_end:
            PySQLPool.commitPool()
        return query.rowcount

    # Execute and returns query result
    @classmethod
    def execute_query(cls, conn, query_string):
        try:
            query = PySQLPool.getNewQuery(conn, commitOnEnd=True)
            query.Query(query_string)

        except MySQLdb.IntegrityError, e:
            logger.error("failed while executing query due to" + str(e.message))
            return None

        except Exception as e:
            logger.error("failed while executing query due to" + e.message)
            return e.message
        return query.record

    # Execute and returns affected rows used for batch call with args
    @classmethod
    def execute_query_with_multiple_args(cls, conn, query_string, args):
        query = PySQLPool.getNewQuery(conn, commitOnEnd=True)
        query.queryMany(query_string, args)
        return query.affectedRows

    @staticmethod
    # insert column_list and values array in mysql if exists it replaces the existing key
    def insert_and_update_entries(table_name, column_list, values , primary_key):
        if Config.get("skip_sql_db_insertion"):
            return True
        try:
            query_head = "INSERT INTO " + table_name + "("
            query_tail = (",".join(str(x) for x in column_list)) + ") VALUES("
            multiple_values_query = (",".join('"' + str(x) + '"' for x in values)) + ")"
            multiple_value_dublicate_check = " ON DUPLICATE KEY UPDATE "
            multiple_dublicate_values = (" , ".join(
                str(x) + " = " + '"' + str(values[column_list.index(x)]) + '"' for x in column_list if
                x != primary_key))
            query_string = query_head + query_tail + multiple_values_query + multiple_value_dublicate_check + multiple_dublicate_values
            logger.debug("Query String for Insert %s", str(query_string))
            QueryExecutor.execute_query(mysql_manager.DbConnections.get_instance().discover_sql_write, query_string)
            return True

        except MySQLdb.IntegrityError, e:
            logger.error("call logs insert2 Sql Exception " + str(e))
            logger.error("Error while insert due to " + str(e.message))
            return False

        except Exception, e:
            logger.error("call logs insert2 Sql Exception " + str(e))
            logger.error("Error while insert due to " + str(e.message))
            return False

    @staticmethod
    # insert column_list and values array in mysql if exists it replaces the existing key
    def replcae_entries(table_name, column_list, values):
        try:
            query_head = "REPLACE INTO " + table_name + "("
            query_tail = (",".join(str(x) for x in column_list)) + ") VALUES("
            multiple_values_query = (",".join('"' + str(x) + '"' for x in values)) + ")"
            query_string = query_head + query_tail + multiple_values_query
            logger.info("Query String for replace call %s", str(query_string))
            QueryExecutor.execute_query(mysql_manager.DbConnections.get_instance().discover_sql_write, query_string)
            return True

        except MySQLdb.IntegrityError, e:
            logger.error("call logs replace Sql Exception " + str(e))
            logger.error("Error while replace call due to " + str(e.message))
            return False

        except Exception, e:
            logger.error("call logs replace Sql Exception " + str(e))
            logger.error("Error while replace call due to " + str(e.message))
            return False
