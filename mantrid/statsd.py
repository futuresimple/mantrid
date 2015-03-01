import logging
import eventlet

from eventlet.green import socket

class Statsd:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.__fail_counter = 0
        self.__suspended = False
        self.__reconnect()
        eventlet.spawn(self.__suspend_check)
        logging.info("StatsD initialized, host: %s:%d" % (self.host, self.port,))

    def __suspend_check(self):
        while True:
            if self.__fail_counter > 3:
                logging.warning("Suspending statsd for 5 seconds")
                self.__suspended = True
                self.__fail_counter = 0
                eventlet.sleep(5)
                self.__suspended = False
            eventlet.sleep(1)

    def __reconnect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((self.host, self.port))

    def __send(self, message):
        if self.__suspended:
            return
        try:
            self.sock.send(message)
        except:
            self.__fail_counter += 1
            try:
                self.__reconnect()
                self.sock.send(message)
            except:
                logging.warning("Sending to statsd failed")
                pass

    def incr(self, *args):
        self.__send("%s:1|c" % ".".join((a.replace(".", "-") for a in args)))
