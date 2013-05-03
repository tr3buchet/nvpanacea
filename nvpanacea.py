import gevent.monkey
gevent.monkey.patch_all(dns=False)

import argparse
import logging
import utils
import sys

import hunter_killer


LOG = logging.getLogger(__name__)
logging.ACTION = 33
logging.addLevelName(33, 'ACTION')
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


# Note(tr3buchet): this is necessary to prevent argparse from requiring the
#                  the 'env' parameter when using -l or --list
class _ListAction(argparse._HelpAction):
    """ListAction used for the -l and --list arguments."""
    def __call__(self, parser, *args, **kwargs):
        """Lists are configured supernova environments."""
        config = utils.get_config_from_file()
        for section in config.sections():
            envheader = '-- %s ' % section
            print envheader.ljust(86, '-')
            for param, value in sorted(config.items(section)):
                print '  %s: %s' % (param.upper().ljust(21), value)
        parser.exit()


def main():
    desc = 'view and modify ports using nvp/melange/nova'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-l', '--list', action=_ListAction,
                       dest='listenvs',
                       help='list all configured environments')
    parser.add_argument('--limit', type=int, action='store',
                        help="limit port selection to number specified",
                        default=None)
    parser.add_argument('--loglevel', action='store',
            help="set log level: DEBUG, INFO, WARN, ACTION, ERROR..",
                        default='ACTION')
    parser.add_argument('-e', '--environment', action='store',
                        help="Environment to run against, for options use -l")
    parser.add_argument('-a', '--action', action='store',
                        help="fix or fixnoop",
                        default='fixnoop')
    parser.add_argument('-t', '--type', action='store',
                        help="orphan_ports, repair_queues, add_vmids, "
                        "orphan_queues")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.loglevel),
                        stream=sys.stdout)

    hk_machine = {'orphan_ports': hunter_killer.OrphanPorts,
                  'repair_queues': hunter_killer.RepairQueues,
                  'no_vmids': hunter_killer.NoVMIDPorts,
                  'orphan_queues': hunter_killer.OrphanQueues}

    if args.type not in hk_machine:
        raise Exception('type not supported, choose in %s' % hk_machine.keys())

    creds = utils.get_connection_creds(args.environment)
    print 'environment: %s' % args.environment
    print 'nvp url: %s' % creds['nvp_url']
    print 'nova mysqljson bridge: %s' % creds['nova_url']
    print 'melange mysqljson bridge: %s' % creds['melange_url']
    print 'iz in yur controller iteratin yur business (%s)' % args.action
    hk = hk_machine[args.type](action=args.action, **creds)
    hk.execute(limit=args.limit)


if __name__ == "__main__":
    main()
