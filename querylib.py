import logging
import time

import requests
from requests.auth import HTTPBasicAuth
requests.adapters.DEFAULT_RETRIES = 5

import aiclib
from utils import IterableQuery


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


class ResourceNotFound(Exception):
    pass


class NVP(object):
    ALL_RELATIONS = ['LogicalPortStatus', 'LogicalPortAttachment',
                     'LogicalQueueConfig', 'LogicalSwitchConfig',
                     'LogicalSwitchStatus',
                     'TransportNodeInterfaceStatus',
                     'VirtualInterfaceConfig']

    def __init__(self, url, username, password):
        self.connection = aiclib.nvp.Connection(url, username=username,
                                                password=password)

        # specifically for self.url_request()
        self.session = requests.session()
        self.url = url
        self.auth = HTTPBasicAuth(username, password)

        self.calls = 0

        # small memory cache to prevent multiple lookups
        self.qos_pools_by_id = {}
        self.qos_pools_by_name = {}
        self.transport_zones = {}

    def url_request(self, url, method='get', payload=None):
        """make a manual request of NVP, will unroll pages if they exist"""
        url = self.url + url
        results = []
        r = self._request_with_retry(url, method, payload)
        if method == 'delete':
            return
        output = r.json()
        results.extend(output.get('results', []))

        # if we got a page_cursor, handle it
        while 'page_cursor' in output:
            payload['_page_cursor'] = output['page_cursor']
            r = self._request_with_retry(url, method, payload)
            output = r.json()
            results.extend(output.get('results', []))
        return results

    def _request_with_retry(self, url, method='get', payload=None):
        http_method = getattr(self.session, method)
        while True:
            try:
                LOG.info('making call |%s - %s| with payload |%s|',
                         method, url, payload)
                self.calls += 1
                r = http_method(url, params=payload, verify=False,
                                auth=self.auth)
                r.raise_for_status()
                return r
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    LOG.error('HTTP exception |%s| |%s %s|' % (e, method, url))
                    raise ResourceNotFound('not found |%s|' % url)
                LOG.error('HTTP exception |%s|, retrying' % e)
                time.sleep(.01)

    @classmethod
    def _check_relations(cls, relations):
        for relation in relations:
            if relation not in cls.ALL_RELATIONS:
                raise Exception('Bad relation requested: %s' % relation)

    @classmethod
    def object_new_tags(cls, obj, tag_scope, tag_value):
        tags = obj.get('tags', {})
        tags['tag_scope'] == tag_value
        return tags

    #################### PORTS ################################################

    def delete_port(self, port):
        self.calls += 1
        self.connection.lswitch_port(port['switch']['uuid'],
                                     port['uuid']).delete()

    def delete_port_manual(self, port):
        url = '/ws.v1/lswitch/%s/lport/%s' % (port['switch']['uuid'],
                                              port['uuid'])
        try:
            self.url_request(url, 'delete')
        except ResourceNotFound:
            pass

    def get_port(self, port):
        # get an nvp port form our local dict object
        self.calls += 1
        query = self.connection.lswitch_port(port['switch']['uuid'],
                                             port['uuid']).read()
        return query or None

    def get_ports(self, relations=None, limit=None, queue_uuid=None):
        query = self.connection.lswitch_port('*').query()

        # append length to query
        # passing this only helps for queries < 1000
        # all greater length queries will use 1000 as page size
        # so 1001 will consume 2 full 1000 port queries
        # this is at this point a silly optimization
        if limit:
            nvp_page_length = 1000 if limit > 1000 else limit
            query = query.length(nvp_page_length)

        if relations:
            # handle relations
            self._check_relations(relations)
            query = query.relations(relations)
            # query = query.fields(['uuid', 'tags'])

        if queue_uuid:
            query = query.queue_uuid('=', queue_uuid)

        return IterableQuery(self, query, limit)

    def get_ports_manual(self, relations=None, queue_uuid=None):
        url = '/ws.v1/lswitch/*/lport'
        payload = {'fields': '*',
                   '_page_length': 1000}
        if relations:
            payload['relations'] = relations
        if queue_uuid:
            payload['queue_uuid'] = queue_uuid
        return self.url_request(url, 'get', payload)

    def get_ports_hashed_by_queue_id(self):
        relations = ('LogicalQueueConfig', )
        queue_hash = {}
        for port in self.get_ports(relations):
            queue_uuid = port['_relations']['LogicalQueueConfig'].get('uuid')
            if queue_uuid:
                if queue_uuid in queue_hash:
                    queue_hash[queue_uuid].append(port)
                else:
                    queue_hash[queue_uuid] = [port]
        return queue_hash

    def port_update_queue(self, port, queue_id):
        self.calls += 1
        port = self.connection.lswitch_port(port['switch']['uuid'],
                                            port['uuid'])
        port.qosuuid(queue_id)
        return port.update()

    def port_update_tag(self, port, tag_scope, tag_value):
        # port passed in needs tags (so only 1 call)
        self.calls += 1
        tags = self.object_new_tags(port, tag_scope, tag_value)

        port_query_obj = self.connection.lswitch_port(port['switch']['uuid'],
                                                      port['uuid'])
        port_query_obj.tags(aiclib.h.tags(tags))
        return port_query_obj.update()

    #################### SWITCHES #############################################

    def get_switch_by_id(self, id):
        self.calls += 1
        return self.connection.lswitch(uuid=id).read() or None

    def get_switches(self, limit=None):
        self.calls += 1
        query = self.connection.lswitch().query()
        return IterableQuery(self, query, limit)

    def switch_update_tag(self, switch, tag_scope, tag_value):
        self.calls += 1
        nvp_switch = self.get_switch_by_id(switch['uuid'])
        tags = self.object_new_tags(nvp_switch, tag_scope, tag_value)

        switch_query_obj = self.connection.lswitch(switch['uuid'])
        switch_query_obj.tags(aiclib.h.tags(tags))

        return switch_query_obj.update()

    #################### QUEUES ############################################

    def get_queues(self, limit=None):
        query = self.connection.qos().query()

        if limit:
            nvp_page_length = 1000 if limit > 1000 else limit
            query = query.length(nvp_page_length)

        return IterableQuery(self, query, limit)

    def get_queues_manual(self):
        url = '/ws.v1/lqueue'
        payload = {'fields': '*',
                   '_page_length': 1000}
        return self.url_request(url, 'get', payload)

    def create_queue(self, display_name, vmid, max_bandwidth_rate):
        self.calls += 1
        queue = self.connection.qos()
        queue.display_name(display_name)
        queue.tags(aiclib.h.tags({'vmid': vmid}))
        queue.maxbw_rate(max_bandwidth_rate)
        return queue.create()

    def delete_queue(self, id):
        self.calls += 1
        self.connection.qos(id).delete()

    def delete_queue_manual(self, id):
        url = '/ws.v1/lqueue/%s' % id
        try:
            self.url_request(url, 'delete')
        except ResourceNotFound:
            pass

    def update_queue_maxbw_rate(self, queue, max_bandwidth_rate):
        self.calls += 1
        queue = self.connection.qos(queue['uuid'])
        queue.maxbw_rate(max_bandwidth_rate)
        return queue.update()

    #################### QOS POOLS ############################################
    # a qos pool is actually a queue but these 2 are special

    def get_qos_pool_by_id(self, id):
        if self.qos_pools_by_id.get(id):
            return self.qos_pools_by_id[id]
        self.calls += 1
        pool = self.connection.qos(uuid=id).read()
        self.qos_pools_by_id[id] = pool
        return pool

    def get_qos_pool_by_id_manual(self, id):
        if self.qos_pools_by_id.get(id):
            return self.qos_pools_by_id[id]
        url = '/ws.v1/lqueue'
        payload = {'fields': '*',
                   'uuid': id}
        r = self.url_request(url, 'get', payload)
        if r:
            self.qos_pools_by_id[id] = r[0]
            return r[0]
        raise ResourceNotFound('QOS POOL |%s|' % id)

    def get_qos_pool_by_name(self, name):
        if self.qos_pools_by_name.get(name):
            return self.qos_pools_by_name[name]
        self.calls += 1
        results = self.connection.qos().query().display_name(name).results()
        try:
            pool = results['results'][0]
        except (IndexError, KeyError):
            return None

        self.qos_pools_by_name[name] = pool
        return pool

    def get_qos_pool_by_name_manual(self, name):
        if self.qos_pools_by_name.get(name):
            return self.qos_pools_by_name[name]
        url = '/ws.v1/lqueue'
        payload = {'fields': '*',
                   'display_name': name}
        r = self.url_request(url, 'get', payload)
        if r:
            self.qos_pools_by_name[name] = r[0]
            return r[0]
        raise ResourceNotFound('QOS POOL |%s|' % name)

    #################### TRANSPORT ZONES ######################################

    def get_transport_zone_by_id(self, id):
        if self.transport_zones.get(id):
            return self.transport_zones[id]
        self.calls += 1
        zone = self.connection.zone(uuid=id).read()
        self.transport_zones[id] = zone
        return zone

    def get_transport_zone_by_id_manual(self, id):
        if self.transport_zones.get(id):
            return self.transport_zones[id]
        url = '/ws.v1/transport-zone'
        payload = {'fields': '*',
                   'uuid': id}
        r = self.url_request(url, 'get', payload)
        if r:
            self.transport_zones[id] = r[0]
            return r[0]
        raise ResourceNotFound('TRANSPORT ZONE |%s|' % id)


class MysqlJsonBridgeEndpoint(object):
    def run_query(self, sql):
        payload = {'sql': sql}
        r = self.session.post(self.url, data=payload,
                              verify=False, auth=self.auth)
        self.calls += 1
        r.raise_for_status()
        return r.json()

    def first_result(self, result):
        try:
            return result['result'][0]
        except (TypeError, IndexError, KeyError):
            return None


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

    def get_interfaces(self):
        select_list = ['interfaces.id', 'mac_addresses.address as mac',
                       'device_id',
                       'group_concat(ip_addresses.address) as ips']
        sql = ('select %s from interfaces left join mac_addresses '
               'on interfaces.id=mac_addresses.interface_id left join '
               'ip_addresses on interfaces.id=ip_addresses.interface_id '
               'group by interfaces.id')
        return self.run_query(sql % ','.join(select_list))['result']

    def get_interfaces_hashed_by_id(self):
        return dict((interface['id'], interface)
                    for interface in self.get_interfaces())

    def get_interfaces_hashed_by_device_id(self):
        return dict((interface['device_id'], interface)
                    for interface in self.get_interfaces())


class Nova(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()
        self.calls = 0

    def get_instance_by_id(self, id, join_flavor=False):
        select_list = ['uuid', 'vm_state', 'terminated_at']
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
        select_list = ['uuid', 'vm_state', 'terminated_at']
        if join_flavor:
            select_list.extend(['instance_type_id', 'rxtx_factor'])
            sql = ('select %s from instances left join instance_types '
                   'on instances.instance_type_id=instance_types.id '
                   'where instances.deleted=0')
        else:
            sql = 'select %s from instances where deleted=0'
        return self.run_query(sql % ','.join(select_list))['result']

    def get_instances_hashed_by_id(self, join_flavor=False):
        return dict((instance['uuid'], instance)
                    for instance in self.get_instances(join_flavor))


class Port(dict):
    def __repr__(self):
        return self.__class__.__name__ + '(' + dict.__repr__(self) + ')'
