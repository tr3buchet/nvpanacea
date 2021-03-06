#import gevent.monkey
#gevent.monkey.patch_all(dns=False)

import argparse
import functools
import logging
import utils
import sys

import hunter_killer


LOG = logging.getLogger(__name__)
logging.ACTION = 25
logging.addLevelName(25, 'ACTION')
logging.OUTPUT = 35
logging.addLevelName(35, 'OUTPUT')
LOG.output = lambda s, *args, **kwargs: LOG.log(35, s, *args, **kwargs)


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
    parser.add_argument('--loglevel', action='store',
                        help='set stdout log level: DEBUG, INFO, WARN, '
                             'ACTION, OUTPUT, ERROR..',
                        default='OUTPUT')
    parser.add_argument('-e', '--environment', action='store',
                        help='Environment to run against, for options use -l')
    parser.add_argument('-a', '--action', action='store',
                        help='fix or noop',
                        default='fixnoop')
    parser.add_argument('-t', '--type', action='store',
                        help='orphan_ports, repair_queues, add_vmids, '
                        'orphan_queues, orphan_interfaces, migrate_quark, '
                        'kill_cell_queues, delete_queue_list')
    parser.add_argument('-c', '--cell', action='store',
                        help='required by kill_cell_queues')
    args = parser.parse_args()

    ff = logging.Formatter('%(asctime)s|%(name)s|'
                           '%(levelname)s -> %(message)s')
    logger = logging.getLogger()
    # NOTE(tr3buchet): action log to file
    ah = logging.FileHandler(utils.get_log_name('~/nvpanacea_logs', 'action'))
    ah.setLevel(logging.ACTION)
    ah.setFormatter(ff)
    logger.addHandler(ah)

    # NOTE(tr3buchet): info log to file
    ih = logging.FileHandler(utils.get_log_name('~/nvpanacea_logs', 'info'))
    ih.setLevel(logging.INFO)
    ih.setFormatter(ff)
    logger.addHandler(ih)

    # NOTE(tr3buchet): standard out
    of = logging.Formatter('%(message)s')
    oh = logging.StreamHandler(sys.stdout)
    oh.setLevel(getattr(logging, args.loglevel))
    oh.setFormatter(of)
    logger.addHandler(oh)

    if args.type == 'kill_cell_queues' and args.cell is None:
        parser.print_help()
        utils.raiselog('ERROR: cell is required by kill_cell_queues:')

    hk_machine = {
        'orphan_ports': hunter_killer.OrphanPorts,
        #'repair_queues': hunter_killer.RepairQueues,
        #'no_vmids': hunter_killer.NoVMIDPorts,
        'orphan_queues': hunter_killer.OrphanQueues,
        'kill_cell_queues': functools.partial(hunter_killer.KillCellQueues,
                                              args.cell),
        'delete_queue_list': functools.partial(hunter_killer.DeleteQueueList,
                                               sys.stdin),
        'vif_id_on_device': hunter_killer.VifIDOnDevice,
        'remove_queue_ref': hunter_killer.RemoveQueueRef,
        'sg_ports': hunter_killer.SGPorts,
        'ip_info': hunter_killer.IPInfo,
    }

    if args.type not in hk_machine:
        utils.raiselog('type not supported, choose in %s' % hk_machine.keys())

    creds = utils.get_connection_creds(args.environment)
    LOG.output('iz in yur controller iteratin yur business (%s)' % args.action)
#    sys.stdout.flush()
    hk = hk_machine[args.type](action=args.action, **creds)
    hk.execute()


if __name__ == '__main__':
    main()
