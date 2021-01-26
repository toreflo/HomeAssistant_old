import logging
import mysql.connector
import voluptuous as vol
import homeassistant.helpers.config_validation as cv

DOMAIN = "mysql_list"
SERVICE_UPDATE = "update"
SERVICE_GET_DATA = "get_data"

CONF_HOST = 'host'
CONF_DATABASE = 'database'
CONF_USERNAME = 'user'
CONF_PASSWORD = 'password'
CONF_TABLE = 'table'
CONF_ENTITY = 'entity_id'

PAR_DEL_FLAG = 'delete'
PAR_NAME = 'name'

_LOGGER = logging.getLogger(__name__)

def setup(hass, config):
    """Set up is called when Home Assistant is loading our component."""
    CONFIG_SCHEMA = vol.Schema({
        vol.Required(CONF_HOST): cv.string,
        vol.Required(CONF_DATABASE): cv.string,
        vol.Required(CONF_USERNAME): cv.string,
        vol.Required(CONF_PASSWORD): cv.string,
        vol.Required(CONF_TABLE): cv.string,
        vol.Required(CONF_ENTITY): cv.string
    })
    host = config[DOMAIN].get(CONF_HOST)
    database = config[DOMAIN].get(CONF_DATABASE)
    username = config[DOMAIN].get(CONF_USERNAME)
    password = config[DOMAIN].get(CONF_PASSWORD)
    table = config[DOMAIN].get(CONF_TABLE)
    entity = config[DOMAIN].get(CONF_ENTITY)

    def create_table_if_not_exists(connection, service_name):
        _LOGGER.info("{}.{}: creating table if not exists {}".format(DOMAIN, service_name, table))
        cursor = connection.cursor()    
        cursor.execute("create table if not exists {} (value varchar(128) primary key)".format(table))
        connection.commit()
        cursor.close()

    def load_entity(connection, service_name):
        _LOGGER.info("{}.{}: loading data in {}".format(DOMAIN, service_name, entity))
        query = "select value from {}".format(table)
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        _LOGGER.debug("Total number of rows in {} is: {}".format(table, cursor.rowcount))
        elems = []
        for row in records:
            _LOGGER.debug("value = {}".format(row[0]))
            elems.append(row[0])
        curr_state = hass.states.get(entity).state
        hass.states.set(entity, curr_state, { "options": elems })

    def insert(connection, service, value):
        _LOGGER.info("{}.{}: insert {}".format(DOMAIN, service, value))
        query = "insert into {} values ('{}')".format(table, value)
        cursor = connection.cursor()    
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def delete(connection, service, value):
        _LOGGER.info("{}.{}: delete {}".format(DOMAIN, service, value))
        query = "delete from {} where value = '{}'".format(table, value)
        cursor = connection.cursor()    
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def get_data(call):
        """Handle the service call."""
        connection = mysql.connector.connect(user=username, password=password, host=host, database=database)
        create_table_if_not_exists(connection, SERVICE_GET_DATA)
        load_entity(connection, SERVICE_GET_DATA)
        connection.close()

    def update_list(call):
        """Handle the service call."""
        del_flag = call.data.get(PAR_DEL_FLAG)
        name = call.data.get(PAR_NAME)
        connection = mysql.connector.connect(user=username, password=password, host=host, database=database)
        create_table_if_not_exists(connection, SERVICE_UPDATE)
        if del_flag:
            delete(connection, SERVICE_UPDATE, name)
        else:
            insert(connection, SERVICE_UPDATE, name)
        load_entity(connection, SERVICE_UPDATE)
        connection.close()

    hass.services.register(DOMAIN, SERVICE_GET_DATA, get_data)
    hass.services.register(DOMAIN, SERVICE_UPDATE, update_list)

    return True
