import logging
from datetime import datetime

from asgiref.sync import async_to_sync
from json import dumps
import websockets

from .handler import Handler

LOGGER = logging.getLogger(__name__)

OPEN_STATE = 1

class WsHandler(Handler):
    """Websocket handler.
    """

    def __init__(self, connection, **kwargs):
        """Constructor.
        """
        super().__init__(**kwargs)
        self._connection = connection
        self._socket = {}

    def _connect_socket(self, key):
        """Connect socket.
        """
        socket = self._socket.get(key)
        if not socket or int(socket.state) != OPEN_STATE:
            LOGGER.info('Connecting %s [%s]', key, socket and socket.state)
            uri = self._connection + key + '/'
            self._socket[key] = async_to_sync(websockets.connect)(uri)

        assert self._socket, "Socket is not initialized"

    def load(self, queue):
        """Load.
        """
        super().load(queue=queue)
        LOGGER.info('Binding connection %s as a publisher',
                    self._connection)

    def create_table(self, table_name, fields, **kwargs):
        """Create table.
        """
        self._connect_socket(table_name)

    def insert(self, table_name, fields):
        """Insert.
        """
        self._connect_socket(table_name)

        native_fields = {
            k: self.serialize(v) for k, v in fields.items()
            if not v.is_auto_increment}

        data = {
            "table_name": table_name,
            "data": native_fields,
        }

        socket = self._socket.get(table_name)
        if socket and int(socket.state) == OPEN_STATE:
            try:
                async_to_sync(self._socket[table_name].send)(dumps(data))
            except Exception as e:
                LOGGER.warning('Socket send %s [%s]', type(e), e)
                del(self._socket[table_name])

    @staticmethod
    def serialize(value):
        """Serialize value.
        """
        if isinstance(value.value, datetime):
            return str(value)

        return value.value
