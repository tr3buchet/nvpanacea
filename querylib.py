import json
import logging
import time

import requests
from requests.auth import HTTPBasicAuth
from requests.packages import urllib3
urllib3.disable_warnings()


LOG = logging.getLogger(__name__)


class ResourceNotFound(Exception):
    pass


class MysqlJsonException(Exception):
    pass


class NVP(object):
    ALL_RELATIONS = ['LogicalPortStatus', 'LogicalPortAttachment',
                     'LogicalQueueConfig', 'LogicalSwitchConfig',
                     'LogicalSwitchStatus',
                     'TransportNodeInterfaceStatus',
                     'VirtualInterfaceConfig']

    def __init__(self, url, username, password):

        # specifically for self.url_request()
        self.session = requests.session()
        self.url = url
#        self.auth = HTTPBasicAuth(username, password)
        self.cookie = self.login(url, username, password)

        self.calls = 0

        # small memory cache to prevent multiple lookups
        self.qos_pools_by_id = {}
        self.qos_pools_by_name = {}
        self.transport_zones = {}

    def login(self, url, username, password, timeout=30):
        URL = url + '/ws.v1/login'
        data = {'username': username, 'password': password}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.post(URL, data=data, headers=headers, verify=False,
                          timeout=timeout)
        return r.headers['set-cookie']

    @staticmethod
    def tags_to_dict(obj):
        """untargs an object's tags"""
        return dict((t['scope'], t['tag']) for t in obj['tags'])

    @staticmethod
    def dict_to_tags(the_d):
        """targs up some tags from a dict"""
        return [{'scope': k, 'tag': v} for k, v in the_d.iteritems()]

    def url_request(self, url, method='get', follow_cursor=True, **kwargs):
        """make a manual request of NVP, will unroll pages if they exist"""
        url = self.url + url
        results = []
        r = self._request_with_retry(url, method, **kwargs)
        if method == 'delete':
            return
        elif method == 'post' or method == 'put':
            return r.json()

        output = r.json()
        if 'results' in output:
            results.extend(output.get('results', []))
        else:
            results.append(output)

        # if we got a page_cursor, handle it
        while follow_cursor and 'page_cursor' in output:
            if 'params' in kwargs:
                kwargs['params']['_page_cursor'] = output['page_cursor']
            else:
                kwargs['params'] = {'_page_cursor': output['page_cursor']}
            r = self._request_with_retry(url, method, **kwargs)
            output = r.json()
            results.extend(output.get('results', []))
        return results

    def _request_with_retry(self, url, method='get', **kwargs):
        http_method = getattr(self.session, method)
        for x in xrange(10):
            try:
                LOG.info('making nvp call |%s - %s| |%s|' %
                         (method, url, kwargs))
                self.calls += 1
                r = http_method(url, headers={'cookie': self.cookie},
                                verify=False,
                                timeout=30, **kwargs)
                r.raise_for_status()
                return r
            except requests.exceptions.Timeout:
                LOG.error('Timeout, retrying. |%s - %s| |%s|' %
                          (method, url, kwargs))
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    LOG.error('HTTP exception |%s| |%s - %s| |%s|' %
                              (e, method, url, kwargs))
                    raise ResourceNotFound('not found |%s|' % url)
                if e.response.status_code == 500:
                    LOG.error('HTTP exception |%s| |%s - %s| |%s|' %
                              (e, method, url, kwargs))
                    raise Exception('server error |%s|' % url)
                LOG.error('HTTP exception |%s| |%s - %s| |%s|, retrying' %
                          (e, method, url, kwargs))
                time.sleep(.01)

    #################### PORTS ################################################

    def delete_port(self, port):
        url = '/ws.v1/lswitch/%s/lport/%s' % (port['switch']['uuid'],
                                              port['uuid'])
        try:
            self.url_request(url, 'delete')
        except ResourceNotFound:
            pass

    def get_ports(self, relations=None, queue_uuid=None, switch_uuid=None):
        url = '/ws.v1/lswitch/%s/lport' % (switch_uuid or '*')
        params = {'fields': '*',
                  '_page_length': 1000}
        if relations:
            params['relations'] = relations
        if queue_uuid:
            params['queue_uuid'] = queue_uuid
        return self.url_request(url, 'get', params=params)
                                #follow_cursor=False)

    def port_update_queue(self, port, queue_id):
        url = '/ws.v1/lswitch/%s/lport/%s' % (port['switch']['uuid'],
                                              port['uuid'])
        data = {'queue_uuid': queue_id}
        try:
            self.url_request(url, 'put', data=json.dumps(data))
        except ResourceNotFound:
            LOG.error('port |%s| was not associated with queue |%s|' %
                      (port['uuid'], queue_id))

    def port_update_tags(self, port):
        url = '/ws.v1/lswitch/%s/lport/%s' % (port['switch']['uuid'],
                                              port['uuid'])
        data = {'tags': self.dict_to_tags(port['tags'])}
        try:
            self.url_request(url, 'put', data=json.dumps(data))
        except ResourceNotFound:
            LOG.error('port |%s| tags were not updated to |%s|' %
                      (port['uuid'], port['tags']))

    def port_delete_queue_ref(self, port):
        url = '/ws.v1/lswitch/%s/lport/%s' % (port['switch']['uuid'],
                                              port['uuid'])
        data = {'queue_uuid': None}
        try:
            self.url_request(url, 'put', data=json.dumps(data))
        except:
            LOG.error('port |%s| failed' % port['uuid'])

    #################### QUEUES ############################################

    def get_queues(self):
        """returns all queues, NOTE: removes qos_pools from list"""
        url = '/ws.v1/lqueue'
        params = {'fields': '*',
                  '_page_length': 1000}
        return [q for q in self.url_request(url, 'get', params=params)
                if self.tags_to_dict(q).get('qos_pool') is None]

    def create_queue(self, display_name, vmid, max_bandwidth_rate):
        url = '/ws.v1/lqueue'
        data = {'display_name': display_name,
                'tags': self.dict_to_tags({'vmid': vmid}),
                'max_bandwidth_rate': max_bandwidth_rate}
        return self.url_request(url, 'post', data=json.dumps(data))

    def delete_queue(self, id):
        url = '/ws.v1/lqueue/%s' % id
        try:
            self.url_request(url, 'delete')
        except ResourceNotFound:
            pass

    def update_queue_maxbw_rate(self, id, max_bandwidth_rate):
        url = '/ws.v1/lqueue/%s' % id
        data = {'max_bandwidth_rate': max_bandwidth_rate}
        try:
            self.url_request(url, 'put', data=json.dumps(data))
        except ResourceNotFound:
            LOG.error('queue |%s| was not found to update!!' % id)

    #################### QOS POOLS ############################################
    # a qos pool is actually a queue but these 2 are special

    def get_qos_pool_by_id(self, id):
        if self.qos_pools_by_id.get(id):
            return self.qos_pools_by_id[id]
        url = '/ws.v1/lqueue'
        params = {'fields': '*',
                  'uuid': id}
        r = self.url_request(url, 'get', params=params)
        if r:
            self.qos_pools_by_id[id] = r[0]
            return r[0]
        raise ResourceNotFound('QOS POOL |%s|' % id)

    def get_qos_pool_by_name(self, name):
        if self.qos_pools_by_name.get(name):
            return self.qos_pools_by_name[name]
        url = '/ws.v1/lqueue'
        params = {'fields': '*',
                  'display_name': name}
        r = self.url_request(url, 'get', params=params)
        if r:
            self.qos_pools_by_name[name] = r[0]
            return r[0]
        raise ResourceNotFound('QOS POOL |%s|' % name)

    #################### TRANSPORT ZONES ######################################

    def get_transport_zone_by_id(self, id):
        if self.transport_zones.get(id):
            return self.transport_zones[id]
        url = '/ws.v1/transport-zone'
        params = {'fields': '*',
                  'uuid': id}
        r = self.url_request(url, 'get', params=params)
        if r:
            self.transport_zones[id] = r[0]
            return r[0]
        raise ResourceNotFound('TRANSPORT ZONE |%s|' % id)


class MysqlJsonBridgeEndpoint(object):
    def run_query(self, sql, hash_by=None):
        data = {'sql': sql}
        LOG.info('|%s|: running sql query |%s|' % (self.url, sql))
        r = self.session.post(self.url, data=data,
                              verify=False, auth=self.auth)
        self.calls += 1
        r.raise_for_status()
        rval = r.json()
        if 'ERROR' in rval:
            LOG.error('|%s|: error running query |%s|' % (self.url,
                                                          rval['ERROR']))
            raise MysqlJsonException(rval['ERROR'])
        if hash_by is None:
            return rval['result']
        else:
            return self.hash_by(rval['result'], hash_by)

    def first_result(self, results):
        try:
            return results[0]
        except (TypeError, IndexError, KeyError):
            return None

    @staticmethod
    def hash_by(results, k):
        return {row[k]: row for row in results}

    def get_table(self, table, hash_by=None):
        return self.run_query('select * from %s' % table, hash_by)

    def show_tables(self):
        return self.run_query('show tables')

    def describe_table(self, table, short=True):
        r = self.run_query('describe %s' % table)
        if short:
            return tuple(item['Field'] for item in r)
        return r


class Melange(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()
        self.calls = 0

    def get_interface_by_id(self, id):
        sql = 'select device_id from interfaces where id="%s"'
        result = self.run_query(sql % id)
        return self.first_result(result)

    def get_interfaces_with_null_viod(self):
        select_list = ['i.id', 'ipb.tenant_id', 'i.vif_id_on_device']
        sql = ('select %s from interfaces as i '
               'JOIN ip_addresses as ipa ON ipa.interface_id = i.id '
               'JOIN ip_blocks as ipb ON ipb.id = ipa.ip_block_id '
               'WHERE i.vif_id_on_device IS NULL')
        return self.run_query(sql % ','.join(select_list))

    def get_interfaces(self):
        select_list = ['interfaces.id', 'mac_addresses.address as mac',
                       'device_id',
                       'group_concat(ip_addresses.address) as ips']
        sql = ('select %s from interfaces left join mac_addresses '
               'on interfaces.id=mac_addresses.interface_id left join '
               'ip_addresses on interfaces.id=ip_addresses.interface_id '
               'group by interfaces.id')
        return self.run_query(sql % ','.join(select_list))

    def get_interfaces_hashed_by_id(self):
        return dict((interface['id'], interface)
                    for interface in self.get_interfaces())

    def get_interfaces_hashed_by_device_id(self):
        return dict((interface['device_id'], interface)
                    for interface in self.get_interfaces())

    def update_interface_viod(self, vif_uuid, port_uuid):
        sql = 'update interfaces set vif_id_on_device="%s" where id="%s"'
        return self.run_query(sql % (vif_uuid, port_uuid))


class Neutron(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()
        self.calls = 0

    def get_ports(self, hash_by='id'):
        return self.get_table('quark_ports', hash_by)

    def get_sg_assoc(self, hash_by='port_id'):
        r = self.get_table('quark_port_security_group_associations')
        if hash_by is None:
            return r
        else:
            d = {}
            for assoc in r:
                if assoc[hash_by] in d:
                    d[assoc[hash_by]].append(assoc)
                else:
                    d[assoc[hash_by]] = []
            return d

    def get_sg(self, hash_by='id'):
        return self.get_table('quark_security_groups', hash_by)

    def get_ip_assoc(self, hash_by='port_id'):
        r = self.get_table('quark_port_ip_address_associations')
        if hash_by is None:
            return r
        else:
            d = {}
            for assoc in r:
                if assoc[hash_by] in d:
                    d[assoc[hash_by]].append(assoc)
                else:
                    d[assoc[hash_by]] = []
            return d

    def get_ip_addresses(self, hash_by='id'):
        return self.get_table('quark_ip_addresses', hash_by)

    def get_subnets(self, hash_by='id'):
        return self.get_table('quark_subnets', hash_by)


class Nova(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()
        self.calls = 0

    def get_instance_by_id(self, id, join_flavor=False):
        select_list = ['uuid', 'vm_state', 'terminated_at', 'cell_name']
        if join_flavor:
            select_list.extend(['instance_type_id', 'rxtx_factor'])
            sql = ('select %s from instances left join instance_types '
                   'on instances.instance_type_id=instance_types.id '
                   'where uuid="%s" and instances.deleted=0')
        else:
            sql = 'select %s from instances where uuid="%s" and deleted=0'
        result = self.run_query(sql % (','.join(select_list), id))
        return self.first_result(result)

    def get_instances(self, join_flavor=False):
        select_list = ['uuid', 'vm_state', 'terminated_at', 'cell_name']
        if join_flavor:
            select_list.extend(['instance_type_id', 'rxtx_factor'])
            sql = ('select %s from instances left join instance_types '
                   'on instances.instance_type_id=instance_types.id '
                   'where instances.deleted=0')
        else:
            sql = 'select %s from instances where deleted=0'
        return self.run_query(sql % ','.join(select_list))

    def get_instances_hashed_by_id(self, join_flavor=False):
        return dict((instance['uuid'], instance)
                    for instance in self.get_instances(join_flavor))
