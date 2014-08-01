import ConfigParser
import keyring
import logging
import os
import re
import socket

LOG = logging.getLogger(__name__)


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
    address = socket.gethostbyname(host)
    parts[2] = address
    return {'url': url, 'resolved_url': '/'.join(parts), 'address': address}


def get_connection_creds(environment):
    config = get_config_from_file()
    msg = ('%s creds not specified. Make sure to set USE_KEYRING specified '
           'values with supernova keyring if you intend to use them')

    print 'environment: %s' % environment

    # get nvp creds
    nvp_url = resolve_url(config.get(environment, 'nvp_url'))
    nvp_user = check_keyring(config.get(environment, 'nvp_user'))
    nvp_pass = check_keyring(config.get(environment, 'nvp_pass'))
    if not (nvp_url and nvp_user and nvp_pass):
        raise Exception(msg % 'nvp')
    print 'nvp url: %s (%s)' % (nvp_url['url'], nvp_url['address'])

    # get melange mysqljsonbridge connection creds
    melange_url = resolve_url(config.get(environment, 'melange_bridge_url'))
    melange_ip = config.get(environment, 'melange_ip')
    melange_port = config.get(environment, 'melange_port')
    melange_ip_user = config.get(environment, 'melange_ip_user')
    melange_ip_pass = config.get(environment, 'melange_ip_pass')
    melange_user = check_keyring(config.get(environment, 'melange_user'))
    melange_pass = check_keyring(config.get(environment, 'melange_pass'))
    if not (melange_url and melange_user and melange_pass):
        raise Exception(msg % 'melange')
    print 'melange mysqljson bridge: %s (%s)' % (melange_url['url'],
                                                 melange_url['address'])

    # get nova mysqljsonbridge connection creds
    nova_url = resolve_url(config.get(environment, 'nova_bridge_url'))
    nova_user = check_keyring(config.get(environment, 'nova_user'))
    nova_pass = check_keyring(config.get(environment, 'nova_pass'))
    if not (nova_url and nova_user and nova_pass):
        raise Exception(msg % 'nova')
    print 'nova mysqljson bridge: %s (%s)' % (nova_url['url'],
                                              nova_url['address'])

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
            'nova_url': nova_url['resolved_url'],
            'nova_username': nova_user,
            'nova_password': nova_pass}
