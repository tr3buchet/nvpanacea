import argparse
import logging
from hunter_killer import HunterKiller
import utils
import sys


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
                        help="orphan_ports, no_queue_ports, or add_vmids",
                        default='no_queue_ports')
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.loglevel),
                        stream=sys.stdout)

    hk = HunterKiller(action=args.action,
                      **utils.get_connection_creds(args.environment))

    print 'iz in yur controller iteratin yur ports (%s)' % args.action

    types = ['orphan_ports', 'no_queue_ports', 'no_vmids']
    if args.type not in types:
        raise Exception('type not supported, choose from %s' % types)
    hk.port_manoeuvre(args.type, args.limit)
    hk.print_calls_made()

#     if args.type == 'orphan':
#         bad_ports = hk.get_orphaned_ports()
#         if args.action == 'list':
#             columns = ('uuid', 'vif_uuid', 'instance_id', 'instance_state',
#                        'instance_terminated_at', 'link_status',
#                        'fabric_status')
#             utils.print_list(bad_ports, columns)
#             print len(bad_ports), 'orphaned ports found'
#             print hk.calls_made()
#         elif args.action in ('fix', 'fixnoop'):
#             for port in bad_ports:
#                 hk.delete_port(port, args.action)
#             print len(bad_ports), 'orphaned ports deleted'
#             print hk.calls_made()
#         return
#     elif args.type == 'no_queue':
#         bad_ports = hk.no_queue_ports(args.action)
#         if args.action == 'list':
#             columns = ('uuid', 'vif_uuid', 'switch_name')
#             utils.print_list(bad_ports, columns)
#         print len(bad_ports), 'queueless ports found'
#         print hk.calls_made()
#     else:
#         raise Exception('type specefication not supported')


if __name__ == "__main__":
    main()
