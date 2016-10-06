#!/usr/bin/python

import utils
import querylib

HKGINOVA = 'hkginova'
DFWINOVA = 'dfwinova'
ORDINOVA = 'ordinova'
LONINOVA = 'loninova'

SYDPROD = 'sydprod'
HKGPROD = 'hkgprod'
IADPROD = 'iadprod'
ORDPROD = 'ordprod'
DFWPROD = 'dfwprod'
LONPROD = 'lonprod'

ORDNETDEV = 'ordnetdev'
ORDPREPROD = 'ordpreprod'



class DoEEET(object):
    def __init__(self, env):
        creds = utils.get_connection_creds(env)
        self.nvp = querylib.NVP(creds['nvp_url'], creds['nvp_username'],
                                creds['nvp_password'])
        self.nova = querylib.Nova(creds['nova_url'], creds['nova_username'],
                                  creds['nova_password'])
        self.melange = querylib.Melange(creds['melange_url'],
                                        creds['melange_username'],
                                        creds['melange_password'])
        self.neutron = querylib.Neutron(creds['neutron_url'],
                                        creds['neutron_username'],
                                        creds['neutron_password'])


class IPInfo(DoEEET):
    def gogogo(self):
        print 'yay!'





if __name__ == '__main__':
    IPInfo(ORDPREPROD).gogogo()

