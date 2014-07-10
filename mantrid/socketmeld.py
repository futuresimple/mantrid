import eventlet
import greenlet
from eventlet.green import socket
from eventlet.timeout import Timeout


class SocketMelder(object):
    """
    Takes two sockets and directly connects them together.
    """

    transmission_timeout_seconds = 30

    def __init__(self, client, server):
        self.client = client
        self.server = server
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

    def run(self):
        self.threads = {
            "ctos": eventlet.spawn(self.piper, self.server, self.client, "client", "stoc"),
            "stoc": eventlet.spawn(self.piper, self.client, self.server, "server", "ctos"),
        }
        try:
            self.threads['stoc'].wait()
        except (greenlet.GreenletExit, socket.error, Timeout):
            pass
        try:
            self.threads['ctos'].wait()
        except (greenlet.GreenletExit, socket.error, Timeout):
            pass

        try:
            self.server.close()
        except:
            logging.exception("Exception caught closing server socket")

        try:
            self.client.close()
        except:
            logging.exception("Exception caught closing client socket")

        return self.data_handled
