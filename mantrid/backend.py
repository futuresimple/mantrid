import eventlet
import logging

from eventlet.green import socket
from eventlet.timeout import Timeout

class Backend(object):

    healthcheck_delay_seconds = 1
    healthcheck_timeout_seconds = 1

    def __init__(self, address_tuple):
        self.address_tuple = address_tuple
        self.active_connections = 0
        self._blacklisted = False 
        self.retired = False
        self._address_repr = ':'.join((self.host, str(self.port)))

    @property
    def blacklisted(self):
        return self._blacklisted

    @blacklisted.setter
    def blacklisted(self, value):
        if value:
            self.start_health_check()
        self._blacklisted = value

    @property
    def address(self):
        return self.address_tuple

    @property
    def address_repr(self):
        return self._address_repr

    def add_connection(self):
        self.active_connections += 1

    def drop_connection(self):
        self.active_connections -= 1

    @property
    def connections(self):
        return self.active_connections

    @property
    def host(self):
        return self.address_tuple[0]

    @property
    def port(self):
        return self.address_tuple[1]

    def __repr__(self):
        return "Backend((%s, %s))" % (self.host, self.port)

    def start_health_check(self):
        eventlet.spawn(self._health_check_loop)

    def _health_check_loop(self):
        while True:
            if self.retired or not self.blacklisted:
                reason = "removing backend" if self.retired else "available"
                logging.warn("Stopping health-checking of %s: %s", self, reason)
                break

            self._check_health()
            eventlet.sleep(self.healthcheck_delay_seconds)

    def _check_health(self):
        logging.debug("Checking health of %s", self)
        try:
            timeout = Timeout(self.healthcheck_timeout_seconds)
            try:
                socket = eventlet.connect((self.host, self.port))
            finally:
                timeout.cancel()

            logging.debug("%s is alive, making sure it is not blacklisted", self)
            self.blacklisted = False
            socket.close()
        except:
            logging.debug("%s seems dead, will check again later", self)

