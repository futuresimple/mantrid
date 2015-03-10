"""
Contains Mantrid's built-in actions.
"""

import errno
import logging
import operator
import os
import random

import eventlet
from eventlet.green import socket
from eventlet.timeout import Timeout
from httplib import responses

from mantrid.backend import Backend
from mantrid.socketmeld import SocketMelder

class NoHealthyBackends(Exception):
    "Poll of usable backends is empty"
    pass

class Action(object):
    "Base action. Doesn't do anything."

    def __init__(self, balancer, host, matched_host):
        self.host = host
        self.balancer = balancer
        self.matched_host = matched_host

    def handle(self, sock, read_data, path, headers):
        raise NotImplementedError("You must use an Action subclass")


class Empty(Action):
    "Sends a code-only HTTP response"

    code = None

    def __init__(self, balancer, host, matched_host, code):
        super(Empty, self).__init__(balancer, host, matched_host)
        self.code = code

    def handle(self, sock, read_data, path, headers):
        "Sends back a static error page."
        try:
            sock.sendall("HTTP/1.0 %s %s\r\nConnection: close\r\nContent-length: 0\r\n\r\n" % (self.code, responses.get(self.code, "Unknown")))
        except socket.error, e:
            if e.errno != errno.EPIPE:
                raise


class Static(Action):
    "Sends a static HTTP response"

    type = None

    def __init__(self, balancer, host, matched_host, type=None):
        super(Static, self).__init__(balancer, host, matched_host)
        if type is not None:
            self.type = type

    # Try to get sendfile() using ctypes; otherwise, fall back
    try:
        import ctypes
        _sendfile = ctypes.CDLL("libc.so.6").sendfile
        _sendfile.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_long, ctypes.c_size_t]
        _sendfile.restype = ctypes.c_ssize_t
    except Exception:
        _sendfile = None

    def handle(self, sock, read_data, path, headers):
        "Sends back a static error page."
        assert self.type is not None
        try:
            # Get the correct file
            try:
                fh = open(os.path.join(self.balancer.static_dir, "%s.http" % self.type))
            except IOError:
                fh = open(os.path.join(os.path.dirname(__file__), "static", "%s.http" % self.type))
            # Send it, using sendfile if poss. (no fileno() means we're probably using mock sockets)
            try:
                self._sendfile(sock.fileno(), fh.fileno(), 0, os.fstat(fh.fileno()).st_size)
            except (TypeError, AttributeError):
                sock.sendall(fh.read())
            # Close the file and socket
            fh.close()
            sock.close()
        except socket.error, e:
            if e.errno != errno.EPIPE:
                raise


class Unknown(Static):
    "Standard class for 'nothing matched'"

    type = "unknown"


class NoHosts(Static):
    "Standard class for 'there are no host entries at all'"

    type = "no-hosts"


class Redirect(Action):
    "Sends a redirect"

    type = None

    def __init__(self, balancer, host, matched_host, redirect_to):
        super(Redirect, self).__init__(balancer, host, matched_host)
        self.redirect_to = redirect_to

    def handle(self, sock, read_data, path, headers):
        "Sends back a static error page."
        if "://" not in self.redirect_to:
            destination = "http%s://%s" % (
                "s" if headers.get('X-Forwarded-Protocol', headers.get('X-Forwarded-Proto', "")).lower() in ("https", "ssl") else "",
                self.redirect_to
            )
        else:
            destination = self.redirect_to
        try:
            sock.sendall("HTTP/1.0 302 Found\r\nLocation: %s/%s\r\n\r\n" % (
                destination.rstrip("/"),
                path.lstrip("/"),
            ))
        except socket.error, e:
            if e.errno != errno.EPIPE:
                raise


class Proxy(Action):
    "Proxies them through to a server. What loadbalancers do."

    attempts = 2
    delay = 1
    default_healthcheck = True
    default_algorithm = "least_connections"
    connection_timeout_seconds = 2

    def __init__(self, balancer, host, matched_host, backends, attempts=None, delay=None, algorithm=default_algorithm, healthcheck=default_healthcheck):
        super(Proxy, self).__init__(balancer, host, matched_host)
        self.host = host
        self.backends = backends
        self.algorithm = algorithm
        self.healthcheck = healthcheck
        self.select_backend = self.random if algorithm == 'random' else self.least_connections
        assert self.backends
        if attempts is not None:
            self.attempts = int(attempts)
        if delay is not None:
            self.delay = float(delay)

    def valid_backends(self):
        return [b for b in self.backends if not b.blacklisted or not self.healthcheck]

    def random(self):
        return random.choice(self.valid_backends())

    def least_connections(self):
        backends = self.valid_backends()
        
        try:
            min_connections = min(b.connections for b in backends)
        except ValueError:
            raise NoHealthyBackends()

        # this is possibly a little bit safer than always returning the first backend
        return random.choice([b for b in backends if b.connections == min_connections])

    def handle(self, sock, read_data, path, headers):
        request_id = headers.get("X-Request-Id", "-")
        for attempt in range(self.attempts):
            if attempt > 0:
                logging.warn("[%s] Retrying connection for host %s", request_id, self.host)

            backend = self.select_backend()
            try:
                timeout = Timeout(self.connection_timeout_seconds)
                try:
                    server_sock = eventlet.connect((backend.host, backend.port))
                finally:
                    timeout.cancel()

                backend.add_connection()
                break
            except socket.error:
                logging.error("[%s] Proxy socket error on connect() to %s of %s", request_id, backend, self.host)
                self.blacklist(backend)
                eventlet.sleep(self.delay)
                continue
            except:
                logging.warn("[%s] Proxy timeout on connect() to %s of %s", request_id, backend, self.host)
                self.blacklist(backend)
                eventlet.sleep(self.delay)
                continue

        # Function to help track data usage
        def send_onwards(data):
            server_sock.sendall(data)
            return len(data)

        try:
            size = send_onwards(read_data)
            size += SocketMelder(sock, server_sock, backend, self.host).run()
        except socket.error, e:
            if e.errno != errno.EPIPE:
                raise
        finally:
            backend.drop_connection()

    def blacklist(self, backend):
        if self.healthcheck and not backend.blacklisted:
            logging.warn("Blacklisting backend %s of %s", backend, self.host)
            backend.blacklisted = True


class Spin(Action):
    """
    Just holds the request open until either the timeout expires, or
    another action becomes available.
    """

    timeout = 120
    check_interval = 1

    def __init__(self, balancer, host, matched_host, timeout=None, check_interval=None):
        super(Spin, self).__init__(balancer, host, matched_host)
        if timeout is not None:
            self.timeout = int(timeout)
        if check_interval is not None:
            self.check_interval = int(check_interval)

    def handle(self, sock, read_data, path, headers):
        "Just waits, and checks for other actions to replace us"
        for i in range(self.timeout // self.check_interval):
            # Sleep first
            eventlet.sleep(self.check_interval)
            # Check for another action
            action = self.balancer.resolve_host(self.host)
            if not isinstance(action, Spin):
                return action.handle(sock, read_data, path, headers)
        # OK, nothing happened, so give up.
        action = Static(self.balancer, self.host, self.matched_host, type="timeout")
        return action.handle(sock, read_data, path, headers)

class Alias(Action):
    """
    Alias for another backend
    """
    def __init__(self, balancer, host, matched_host, hostname, **_kwargs):
        self.host = host
        self.balancer = balancer
        self.matched_host = matched_host
        self.hostname = hostname

        action, kwargs, allow_subs = self.balancer.hosts[self.hostname]
        action_class = self.balancer.action_mapping[action]
        self.aliased = action_class(balancer = self.balancer, host = self.host, matched_host = self.matched_host, **kwargs)

    def handle(self, **kwargs):
        return self.aliased.handle(**kwargs)
