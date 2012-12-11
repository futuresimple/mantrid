import eventlet
import logging

from eventlet.green import socket

class Backend(object):

    health_check_delay_seconds = 1

    def __init__(self, address_tuple):
        self.address_tuple = address_tuple
        self.active_connections = 0
        self.blacklisted = False 
        self.retired = False

    @property
    def address(self):
        return self.address_tuple

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
            if self.retired:
                logging.warn("Stopping health-checking of %s", self)
                break

            logging.debug("Checking health of %s", self)
            try:
                socket = eventlet.connect((self.host, self.port))
                logging.debug("%s is alive, making sure it is not blacklisted", self)
                self.blacklisted = False
                socket.close()
            except:
                logging.debug("%s seems dead, will check again later", self)
                pass

            eventlet.sleep(self.health_check_delay_seconds)

