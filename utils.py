import ConfigParser
import keyring
import logging
import os
import re
import socket
import time

LOG = logging.getLogger(__name__)
LOG.output = lambda s, *args, **kwargs: LOG.log(35, s, *args, **kwargs)


def raiselog(msg):
    LOG.critical(msg)
    raise Exception(msg)


def get_log_name(log_location, prefix=None):
    log_location = os.path.expanduser(log_location)
    if not os.path.exists(log_location):
        os.makedirs(log_location)
    return '%s/%s_%s.log' % (log_location, prefix or '',
                             time.strftime('%Y%m%d-%H%M%S'))


def get_config_from_file():
        possible_configs = [os.path.expanduser("~/.nvpanacea"),
                            '.nvpanacea']
        config = ConfigParser.RawConfigParser()
        config.read(possible_configs)
        if len(config.sections()) < 1:
            return None
        return config


def check_keyring(value):
    if value.startswith('USE_KEYRING'):
        identifier = re.match("USE_KEYRING\['(.*)'\]", value).group(1)
        username = '%s:%s' % ('global', identifier)
        return keyring.get_password('supernova', username)
    return value


def resolve_url(url):
    parts = url.split('/')
    host = parts[2]
    print host
    address = socket.gethostbyname(host)
    parts[2] = address
    return {'url': url, 'resolved_url': '/'.join(parts), 'address': address}


def get_connection_creds(environment):
    config = get_config_from_file()
    print config.get(environment, 'nvp_url')
    msg = ('%s creds not specified. Make sure to set USE_KEYRING specified '
           'values with supernova keyring if you intend to use them')

    LOG.output('environment: %s' % environment)

    # get nvp creds
    nvp_url = resolve_url(config.get(environment, 'nvp_url'))
    nvp_user = check_keyring(config.get(environment, 'nvp_user'))
    nvp_pass = check_keyring(config.get(environment, 'nvp_pass'))
    if not (nvp_url and nvp_user and nvp_pass):
        raiselog(msg % 'nvp')
    LOG.output('nvp url: %s (%s)' % (nvp_url['url'], nvp_url['address']))

    # get melange mysqljsonbridge connection creds
    melange_url = resolve_url(config.get(environment, 'melange_bridge_url'))
    melange_ip = config.get(environment, 'melange_ip')
    melange_port = config.get(environment, 'melange_port')
    melange_ip_user = config.get(environment, 'melange_ip_user')
    melange_ip_pass = config.get(environment, 'melange_ip_pass')
    melange_user = check_keyring(config.get(environment, 'melange_user'))
    melange_pass = check_keyring(config.get(environment, 'melange_pass'))
    if not (melange_url and melange_user and melange_pass):
        raiselog(msg % 'melange')
    LOG.output('melange mysqljson bridge: %s (%s)' % (melange_url['url'],
                                                      melange_url['address']))

    # get nova mysqljsonbridge connection creds
    nova_url = resolve_url(config.get(environment, 'nova_bridge_url'))
    nova_user = check_keyring(config.get(environment, 'nova_user'))
    nova_pass = check_keyring(config.get(environment, 'nova_pass'))
    if not (nova_url and nova_user and nova_pass):
        raiselog(msg % 'nova')
    LOG.output('nova mysqljson bridge: %s (%s)' % (nova_url['url'],
                                                   nova_url['address']))

    # get ineutron mysqljsonbridge connection creds
    neutron_url = resolve_url(config.get(environment, 'neutron_bridge_url'))
    neutron_user = check_keyring(config.get(environment, 'neutron_user'))
    neutron_pass = check_keyring(config.get(environment, 'neutron_pass'))
    if not (neutron_url and neutron_user and neutron_pass):
        raiselog(msg % 'neutron')
    LOG.output('neutron mysqljson bridge: %s (%s)' % (neutron_url['url'],
                                                      neutron_url['address']))

    return {'nvp_url': nvp_url['resolved_url'],
            'nvp_username': nvp_user,
            'nvp_password': nvp_pass,
            'melange_url': melange_url['resolved_url'],
            'melange_ip': melange_ip,
            'melange_port': melange_port,
            'melange_ip_user': melange_ip_user,
            'melange_ip_pass': melange_ip_pass,
            'melange_username': melange_user,
            'melange_password': melange_pass,
            'neutron_url': neutron_url['resolved_url'],
            'neutron_username': neutron_user,
            'neutron_password': neutron_pass,
            'nova_url': nova_url['resolved_url'],
            'nova_username': nova_user,
            'nova_password': nova_pass}
