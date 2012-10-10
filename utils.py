import ConfigParser
import keyring
import logging
import os
import prettytable
import re


LOG = logging.getLogger(__name__)


def print_list(items, columns):
    t = prettytable.PrettyTable(columns)
    t.align = 'l'
    for item in items:
        t.add_row([item.get(c, '') for c in columns])
    print t


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


def get_connection_creds(environment):
    config = get_config_from_file()
    msg = ('%s creds not specified. Make sure to set USE_KEYRING specified '
           'values with supernova keyring if you intend to use them')

    # get nvp creds
    nvp_url = config.get(environment, 'nvp_url')
    nvp_user = check_keyring(config.get(environment, 'nvp_user'))
    nvp_pass = check_keyring(config.get(environment, 'nvp_pass'))
    if not (nvp_url and nvp_user and nvp_pass):
        raise Exception(msg % 'nvp')

    # get melange mysqljsonbridge connection creds
    melange_url = config.get(environment, 'melange_bridge_url')
    melange_user = check_keyring(config.get(environment, 'melange_user'))
    melange_pass = check_keyring(config.get(environment, 'melange_pass'))
    if not (melange_url and melange_user and melange_pass):
        raise Exception(msg % 'melange')

    # get nova mysqljsonbridge connection creds
    nova_url = config.get(environment, 'nova_bridge_url')
    nova_user = check_keyring(config.get(environment, 'nova_user'))
    nova_pass = check_keyring(config.get(environment, 'nova_pass'))
    if not (nova_url and nova_user and nova_pass):
        raise Exception(msg % 'nova')

    return {'nvp_url': nvp_url,
            'nvp_username': nvp_user,
            'nvp_password': nvp_pass,
            'melange_url': melange_url,
            'melange_username': melange_user,
            'melange_password': melange_pass,
            'nova_url': nova_url,
            'nova_username': nova_user,
            'nova_password': nova_pass}


class IterableQuery(object):
    """Iterates over query object multiple batches, supports limit"""
    def __init__(self, nvp, query, limit=None):
        self.nvp = nvp
        self.query = query
        self.first = True
        self.limit = limit or 9999999

    def __iter__(self):
        counter = 0
        batch = self.get_next_batch()
        while batch:
            for item in batch:
                if counter >= self.limit:
                    raise StopIteration()
                counter += 1
                yield item
            if counter == self.limit:
                # if we're at the limit don't don't get next batch
                raise StopIteration()
            batch = self.get_next_batch()

    def get_next_batch(self):
        try:
            self.nvp.calls += 1
            if self.first:
                self.first = False
                return self.query.results()['results']
            return self.query.next()['results']
        except TypeError:
            self.nvp.calls -= 1
            raise StopIteration()
