import logging

import eventlet
import greenlet

from eventlet.green import socket
from eventlet.timeout import Timeout

class SocketMelder(object):
    """
    Takes two sockets and directly connects them together.
    """

    transmission_timeout_seconds = 30

    def __init__(self, client, server, backend, host):
        self.client = client
        self.server = server
        self.backend = backend
        self.host = host
        self.data_handled = 0

    def piper(self, in_sock, out_sock, out_addr, onkill):
        "Worker thread for data reading"
        try:
            timeout = Timeout(self.transmission_timeout_seconds)
            try:
                while True:
                    written = in_sock.recv(32768)
                    if not written:
                        try:
                            out_sock.shutdown(socket.SHUT_WR)
                        except socket.error:
                            self.threads[onkill].kill()
                        break
                    try:
                        out_sock.sendall(written)
                    except socket.error:
                        pass
                    self.data_handled += len(written)
            finally:
                timeout.cancel()
        except greenlet.GreenletExit:
            return
        except Timeout:
            # This one prevents only from closing connection without any data nor status code returned
            # from mantrid when no data was received from backend.
            # When it happens, nginx reports 'upstream prematurely closed connection' and returns 500,
            # and want to have our custom error page to know when it happens. 

            if onkill == "stoc" and self.data_handled == 0:
                out_sock.sendall("HTTP/1.0 594 Backend timeout\r\nConnection: close\r\nContent-length: 0\r\n\r\n")
            logging.warn("Timeout serving request to backend %s of %s", self.backend, self.host)
            return

    def run(self):
        # Two pipers == repeated logging of timeouts
        self.threads = {
            "ctos": eventlet.spawn(self.piper, self.server, self.client, "client", "stoc"),
            "stoc": eventlet.spawn(self.piper, self.client, self.server, "server", "ctos"),
        }

        try:
            self.threads['stoc'].wait()
        except (greenlet.GreenletExit, socket.error):
            pass

        try:
            self.threads['ctos'].wait()
        except (greenlet.GreenletExit, socket.error):
            pass

        try:
            self.server.close()
        except:
            logging.error("Exception caught closing server socket, backend %s of %s", self.backend, self.host)

        try:
            self.client.close()
        except:
            logging.error("Exception caught closing client socket, backend: %s of %s", self.backend, self.host)

        return self.data_handled
