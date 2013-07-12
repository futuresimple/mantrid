#!/usr/bin/env python
#
# Command-line wrapper for mantrid-client used to automate certain
# painful tasks. This way because it's faster than doing the whole
# API thing and I'm hard pressed to get it done. Sorry!
#

import collections
import itertools
import subprocess
import optparse
import re

CLIENT_BINARY = "mantrid-client"

def list_command():
    return [CLIENT_BINARY, "list"]

def set_command(host, backends):
    return [CLIENT_BINARY, "set", host, "proxy", "true", "backends=%s" % (",".join(backends))]

LISTING_LINE_TEST = "tasksmesh.futuresimple.com          proxy[algorithm=least_connections,healthcheck=True]<10.196.153.235:8000,10.196.153.235:8001> True"
LISTING_LINE_RE = re.compile('(?P<name>[.\w-]+)\s+(?P<action>\w+)\[(?P<options>[\w=_,]+)\]<(?P<backends>[.\d:,]+)>\s+(?P<subdomains>\w+)')

assert LISTING_LINE_RE.match(LISTING_LINE_TEST)


def mantrid_configuration():
    p = subprocess.Popen(list_command(), stdout=subprocess.PIPE)
    configuration = p.communicate()[0].split('\n')
    # drop the headers
    if configuration:
        configuration = configuration[1:]
    configuration = [line for line in configuration if line.strip()]
    return [LISTING_LINE_RE.match(line).groupdict() for line in configuration]
    

def mantrid_set(host, backends, dry_run=True, interactive=False):
    verb = "would call" if dry_run else "calling"
    print "%s: %s" % (verb, ' '.join(set_command(host, backends)), )
    if not dry_run:
        really_run = True
        if interactive:
            print "Confirm? [y/N]", 
            yes_no = raw_input()
            really_run = yes_no.strip().lower() == 'y'

        if really_run:
            return subprocess.call(set_command(host, backends))


def hostname(fqdn):
    return fqdn.split('.')[0]


def unique_different_pairs(sequence):
    for i, k in enumerate(sequence):
        for j, l in enumerate(sequence[i + 1:]):
            if k == l: continue
            yield (k, l)


def sanity_check():
    hosts = mantrid_configuration()
    host_backends = collections.defaultdict(set)

    for host in hosts:
        fqdn = host["name"]
        name = hostname(fqdn)
        backends = host["backends"].split(',')

        if name in host_backends:
            if sorted(host_backends[name]) != sorted(backends):
                print "Backends for %s don't match %s: %s != %s" % (fqdn, name, ",".join(sorted(backends)), ",".join(sorted(host_backends[name])))
        else:
            host_backends[name] = backends

    for host_backends1, host_backends2 in unique_different_pairs(host_backends.items()):
        host1, host2 = host_backends1[0], host_backends2[0]
        backends1, backends2 = host_backends1[1], host_backends2[1]

        if host1.startswith(host2) or host2.startswith(host1):
            continue

        for backend in backends1: 
            if backend in backends2:
                print "Hosts %s and %s share backend %s" % (host1, host2, backend)


def remove_host(host_to_remove, dry_run, interactive):
    hosts = mantrid_configuration()
    for host in hosts:
        backends = host["backends"].split(',')
        hosts_ports = [tuple(backend.split(':')) for backend in backends]
        new_host_ports = [(name, port) for name, port in hosts_ports if name != host_to_remove]

        if hosts_ports != new_host_ports:
            new_backends = ["%s:%s" % (name, port) for name, port in new_host_ports]
            mantrid_set(host["name"], new_backends, dry_run, interactive)
        

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-r", "--remove-backend", dest="backend_to_remove", help="Remove the given backend from all hosts", metavar="ip:port")
    parser.add_option("-s", "--sanity-check", dest="sanity_check", help="Check current configuration for sanity", action="store_true")
    parser.add_option("-d", "--dry-run", dest="dry_run", help="Show what would happen, without excecuting", action="store_true", default=False)
    parser.add_option("-i", "--interactive", dest="interactive", help="Prompt before every change", action="store_true", default=False)

    (options, args) = parser.parse_args()

    if options.sanity_check:
        sanity_check()
    if options.backend_to_remove:
        remove_host(options.backend_to_remove, dry_run=options.dry_run, interactive=options.interactive)

