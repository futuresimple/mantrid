import sys

from mantrid.actions import Proxy
from mantrid.backend import Backend
from mantrid.client import MantridClient


class MantridCli(object):
    """Command line interface to Mantrid"""

    def __init__(self, base_url):
        self.client = MantridClient(base_url)

    @classmethod
    def main(cls):
        cli = cls("http://localhost:8042")
        cli.run(sys.argv)

    @property
    def action_names(self):
        for method_name in dir(self):
            if method_name.startswith("action_") \
               and method_name != "action_names":
                yield method_name[7:]
        
    def run(self, argv):
        # Work out what action we're doing
        try:
            action = argv[1]
        except IndexError:
            sys.stderr.write(
                "Please provide an action (%s).\n" % (
                    ", ".join(self.action_names),
                )
            )
            sys.exit(1)
        if action not in list(self.action_names):
            sys.stderr.write(
                "Action %s does not exist.\n" % (
                    action,
                )
            )
            sys.exit(1)
        # Run it
        getattr(self, "action_%s" % action)(*argv[2:])
    
    def action_list(self):
        "Lists all hosts on the LB"
        format = "%-35s %-25s %-8s"
        print format % ("HOST", "ACTION", "SUBDOMS")
        for host, details in sorted(self.client.get_all().items()):
            if details[0] in ("proxy", "mirror"):
                action = "%s[algorithm=%s,healthcheck=%s]<%s>" % (
                    details[0],
                    details[1].get('algorithm', Proxy.default_algorithm),
                    details[1].get('healthcheck', Proxy.default_healthcheck),
                    ",".join(
                        "%s:%s" % (backend.host, backend.port)
                        for backend in details[1]['backends']
                    )
                )
            elif details[0] == "static":
                action = "%s<%s>" % (
                    details[0],
                    details[1]['type'],
                )
            elif details[0] == "redirect":
                action = "%s<%s>" % (
                    details[0],
                    details[1]['redirect_to'],
                )
            elif details[0] == "empty":
                action = "%s<%s>" % (
                    details[0],
                    details[1]['code'],
                )
            elif details[0] == "alias":
                action = "%s<%s>" % (
                    details[0],
                    details[1]['hostname'],
                )
            else:
                action = details[0]
            print format % (host, action, details[2])
    
    def action_set(self, hostname=None, action=None, subdoms=None, *args):
        "Adds a hostname to the LB, or alters an existing one"
        usage = "set <hostname> <action> <subdoms> [option=value, ...]"
        if hostname is None:
            sys.stderr.write("You must supply a hostname.\n")
            sys.stderr.write("Usage: %s\n" % usage)
            sys.exit(1)
        if action is None:
            sys.stderr.write("You must supply an action.\n")
            sys.stderr.write("Usage: %s\n" % usage)
            sys.exit(1)
        if subdoms is None or subdoms.lower() not in ("true", "false"):
            sys.stderr.write("You must supply True or False for the subdomains flag.\n")
            sys.stderr.write("Usage: %s\n" % usage)
            sys.exit(1)
        # Grab options
        options = {}
        for arg in args:
            if "=" not in arg:
                sys.stderr.write("%s is not a valid option (no =)\n" % (
                    arg
                ))
                sys.exit(1)
            key, value = arg.split("=", 1)
            options[key] = value
        # Sanity-check options
        if action in ("proxy, mirror") and "backends" not in options:
            sys.stderr.write("The %s action requires a backends option.\n" % action)
            sys.exit(1)
        if action == "alias" and "hostname" not in options:
            sys.stderr.write("The %s action requires hostname option.\n" % action)
            sys.exit(1)
        if "healthcheck" in options and options["healthcheck"].lower() not in ("true", "false"):
            sys.stderr.write("The healthcheck option must be one of (true, false)")
            sys.exit(1)
        if action == "static" and "type" not in options:
            sys.stderr.write("The %s action requires a type option.\n" % action)
            sys.exit(1)
        if action == "redirect" and "redirect_to" not in options:
            sys.stderr.write("The %s action requires a redirect_to option.\n" % action)
            sys.exit(1)
        if action == "empty" and "code" not in options:
            sys.stderr.write("The %s action requires a code option.\n" % action)
            sys.exit(1)
        # Expand some options from text to datastructure
        if "backends" in options:
            options['backends'] = [
                Backend((lambda x: (x[0], int(x[1])))(bit.split(":", 1)))
                for bit in options['backends'].split(",")
            ]
        if "healthcheck" in options:
            options['healthcheck'] = (options['healthcheck'].lower() == "true")
        # Set!
        self.client.set(
            hostname,
            [action, options, subdoms.lower() == "true"]
        )
    
    def action_delete(self, hostname):
        "Deletes the hostname from the LB."
        self.client.delete(
            hostname,
        )
    
    def action_stats(self, hostname=None):
        "Shows stats (possibly limited by hostname)"
        format = "%-35s %-11s %-11s %-11s %-11s"
        print format % ("HOST", "OPEN", "COMPLETED", "BYTES IN", "BYTES OUT")
        for host, details in sorted(self.client.stats(hostname).items()):
            print format % (
                host,
                details.get("open_requests", 0),
                details.get("completed_requests", 0),
                details.get("bytes_received", 0),
                details.get("bytes_sent", 0),
            )

if __name__ == "__main__":
    MantridCli.main()
