import logging

import requests
from requests.auth import HTTPBasicAuth

import aiclib
from utils import IterableQuery


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


class NVP(object):
    ALL_RELATIONS = ['LogicalPortStatus', 'LogicalPortAttachment',
                     'LogicalQueueConfig', 'LogicalSwitchConfig',
                     'LogicalSwitchStatus',
                     'TransportNodeInterfaceStatus',
                     'VirtualInterfaceConfig']

    def __init__(self, url, username, password):
        self.connection = aiclib.nvp.Connection(url, username=username,
                                                     password=password)
        self.calls = 0

        # small memory cache to prevent multiple lookups
        self.qos_pools_by_id = {}
        self.qos_pools_by_name = {}
        self.transport_zones = {}

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

    def get_port(self, port):
        # get an nvp port form our local dict object
        self.calls += 1
        query = self.connection.lswitch_port(port['switch']['uuid'],
                                             port['uuid']).read()
        return query or None

    def get_ports(self, relations=None, limit=None):
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

        return IterableQuery(self, query, limit)

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

    #################### TRANSPORT ZONES ######################################

    def get_transport_zone_by_id(self, id):
        if self.transport_zones.get(id):
            return self.transport_zones[id]
        self.calls += 1
        zone = self.connection.zone(uuid=id).read()
        self.transport_zones[id] = zone
        return zone


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
                   'where uuid="%s"')
        else:
            sql = 'select %s from instances where uuid="%s" and deleted=0'
        result = self.run_query(sql % (','.join(select_list), id))
        return self.first_result(result)


class Port(dict):
    def __repr__(self):
        return self.__class__.__name__ + '(' + dict.__repr__(self) + ')'
