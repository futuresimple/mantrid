
class Backend(object):
    def __init__(self, address_tuple):
        self.address_tuple = address_tuple
        self.active_connections = 0

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

