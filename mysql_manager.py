__author__ = 'harsh-verma'

import PySQLPool
from discover.utils.log import Logger
from discover.system_config import Config

logger = Logger.get_logger(__file__)


class DbConnections():
    __dbconnections = None

    def __init__(self):
        self.discover_sql_read = SqlManager.get_db_connection(SqlDbInstances.DISCOVER_DS, True)
        self.discover_sql_write = SqlManager.get_db_connection(SqlDbInstances.DISCOVER_DS, False)

    @classmethod
    def get_instance(cls):
        if not cls.__dbconnections:
            cls.__dbconnections = DbConnections()
        return cls.__dbconnections


class SqlDbInstances:
    HIKE_DISCOVER_DS = 'discoverds'


class SqlManager:
    __sql_read_connection_pools = dict()
    __sql_write_connection_pools = dict()
    __sql_config = None

    @staticmethod
    def get_sql_config():
        if SqlManager.__sql_config is None:
            SqlManager.__sql_config = Config.get('sql')
        return SqlManager.__sql_config

    @staticmethod
    def get_db_properties(instance_name):
        return SqlDbProperties(instance_name)

    @staticmethod
    def create_connection_pool(db_properties, is_read):
        PySQLPool.getNewPool().maxActiveConnections = SqlDbProperties.READ_MAX_CONNECTIONS if is_read else SqlDbProperties.WRITE_MAX_CONNECTIONS
        conn_pool = PySQLPool.getNewConnection(username=db_properties.db_username,
                                               password=db_properties.db_pass,
                                               host=db_properties.read_host if is_read else db_properties.write_host,
                                               port=db_properties.port,
                                               db=db_properties.db_name,
                                               charset=db_properties.charset,
                                               use_unicode=db_properties.use_unicode,
                                               connect_timeout =db_properties.connect_timeout,
                                               commitOnEnd = db_properties.commitOnEnd
                                               )
        if is_read:
            SqlManager.__sql_read_connection_pools[db_properties.db_name] = conn_pool
        else:
            SqlManager.__sql_write_connection_pools[db_properties.db_name] = conn_pool

        return conn_pool

    @staticmethod
    def get_db_connection(db_name, is_read):
        connection = SqlManager.__sql_write_connection_pools.get(db_name, None)
        if is_read:
            connection = SqlManager.__sql_read_connection_pools.get(db_name, None)

        if connection is not None:
            return connection

        return SqlManager.create_connection_pool(SqlDbProperties(db_name), is_read)


class SqlDbProperties:
    READ_MAX_CONNECTIONS = 15
    WRITE_MAX_CONNECTIONS = 15
    TIMEOUT = 10

    def __init__(self, db_name, config=None):
        sql_config = SqlManager.get_sql_config() if config is None else config
        db_config = sql_config.get(db_name, {})
        self.read_host = db_config.get('read.host', "10.9.51.9")
        self.write_host = db_config.get('write.host', "10.9.51.11")
        self.port = db_config.get('port', 3306)
        self.db_username = db_config.get('user', "datascience")
        self.db_pass = db_config.get('passwd', "discoverds")
        self.db_name = db_config.get("db", "discoverds")
        self.charset = db_config.get('charset', "utf8")
        self.use_unicode = db_config.get('use_unicode', True)
        self.connect_timeout = db_config.get('connect_timeout', 10)
        self.commitOnEnd = db_config.get('commit_on_end', False)
