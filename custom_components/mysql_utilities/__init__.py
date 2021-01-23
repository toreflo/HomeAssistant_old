import logging
import mysql.connector
import voluptuous as vol
import homeassistant.helpers.config_validation as cv

DOMAIN = "mysql_list"
SERVICE_UPDATE = "update"

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

    def update_list(call):
        """Handle the service call."""

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

        del_flag = call.data.get(PAR_DEL_FLAG)
        name = call.data.get(PAR_NAME)

        if del_flag:
            query = "delete from {} where value = '{}'".format(table, name)
        else:
            query = "insert into {} (value) values ('{}')".format(table, name)
        _LOGGER.info("{}.{}: executing >>> {} <<<".format(DOMAIN, SERVICE_UPDATE, query))
        
        connection = mysql.connector.connect(user=username, password=password, host=host, database=database)

        cursor = connection.cursor()    
        cursor.execute(query)
        something = cursor.lastrowid
        _LOGGER.info("{}.{}: After query, Something = {}".format(DOMAIN, SERVICE_UPDATE, something))
        connection.commit()
        cursor.close()

        query = "select value from {}".format(table)
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        _LOGGER.info("Total number of rows in {} is: {}".format(table, cursor.rowcount))
    
        elems = []
        for row in records:
            _LOGGER.info("value = {}".format(row[0]))
            elems.append(row[0])

        connection.close()
        hass.states.set(entity, name, { "options": elems })
        _LOGGER.info("{}.{}: finish".format(DOMAIN, SERVICE_UPDATE))

    hass.services.register(DOMAIN, SERVICE_UPDATE, update_list)

    return True
