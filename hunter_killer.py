import aiclib
import logging
import re
import requests
from requests.auth import HTTPBasicAuth
from utils import IterableQuery


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


zone_qos_pool_map = {'public': 'pub_base_rate',
                     'private': 'snet_base_rate'}


class HunterKiller(object):
    def __init__(self, nvp_url, nvp_username, nvp_password,
                       nova_url, nova_username, nova_password,
                       melange_url, melange_username, melange_password):
        self.nvp = NVP(nvp_url, nvp_username, nvp_password)
        self.nova = Nova(nova_url, nova_username, nova_password)
        self.melange = Melange(melange_url, melange_username, melange_password)

    def get_instance_by_port(self, port, join_flavor=False):
        interface = self.melange.get_interface_by_id(port['vif_uuid']) \
                    if port['vif_uuid'] else None
        return self.nova.get_instance_by_id(interface['device_id'],
                                            join_flavor) \
               if interface and interface['device_id'] else None

    def delete_port(self, port, action):
        LOG.action('delete port |%s|', port['uuid'])
        if action == 'fix':
            return self.nvp.delete_port(port)

    def get_orphaned_ports(self):
        relations = ('LogicalPortStatus', 'LogicalPortAttachment')
        ports = self.nvp.get_ports(relations, limit=20)

        bad_port_list = []
        for port in ports:
            status = port['_relations']['LogicalPortStatus']
            attachment = port['_relations']['LogicalPortAttachment']
            lstatus = 'up' if status['link_status_up'] else 'down'
            fstatus = 'up' if status['fabric_status_up'] else 'down'
            port_dict = {'uuid': port.get('uuid', ''),
                         'lswitch_uuid': status['lswitch']['uuid'],
                         'vif_uuid': attachment.get('vif_uuid', ''),
                         'link_status': lstatus,
                         'fabric_status': fstatus}

            # get the instance
            instance = self.get_instance_by_port(port_dict)
            if instance:
                port_dict['instance_id'] = instance['uuid']
                port_dict['instance_state'] = instance['vm_state']
                port_dict['instance_terminated_at'] = \
                        instance['terminated_at'] or ''
            else:
                port_dict['instance_id'] = None
                port_dict['instance_state'] = None
                port_dict['instance_terminated_at'] = None

            # only return ports with no instance
            # TODO: only return ports if instance_terminated_at > x hours
            if not instance or instance['vm_state'] == 'deleted':
                bad_port_list.append(port_dict)

        return bad_port_list

##########################################

    def get_tag(self, tags, tag_name):
        for tag in tags:
            if tag['scope'] == tag_name:
                return tag['tag']
        return None

    def is_tenant_switch(self, switch_tags):
        os_tid = self.get_tag(switch_tags, 'os_tid')
        return not re.search('-c[0-9]{4}$', os_tid)

    def get_qos_pool_from_switch_tags(self, tags):
        qos_pool_id = self.get_tag(tags, 'qos_pool')
        if qos_pool_id:
            return self.nvp.get_qos_pool_by_id(qos_pool_id)
        return None

    def get_qos_pool_from_transport_zone_map(self, zone_id):
        zone = self.nvp.get_transport_zone_by_id(zone_id)
        zone_name = zone['display_name']
        qos_pool_name = zone_qos_pool_map[zone_name]
        return self.nvp.get_qos_pool_by_name(qos_pool_name)

    def get_qos_pool(self, port):
        qos_pool = self.get_qos_pool_from_switch_tags(port['switch']['tags'])
        if qos_pool:
            return qos_pool

        msg = 'port |%s| switch |%s||%s| does not have a qos pool!'
        LOG.error(msg, port['uuid'], port['switch']['uuid'],
                  port['switch']['name'])

        # lswitch didn't have a qos_pool, have to use transport zone
        zone_id = port['switch']['transport_zone_uuid']
        qos_pool = self.get_qos_pool_from_transport_zone_map(zone_id)
        if qos_pool:
            return qos_pool

        msg = "qos pool couldn't be found using transport zone map either!"
        LOG.error(msg, port['uuid'], port['switch']['uuid'],
                  port['switch']['name'])

    def repair_port_queue(self, port, action):
        LOG.action('fix queue for port |%s|', port['uuid'])
        if port['rxtx_cap']:
            LOG.warn('port |%s| already has a queue!', port['uuid'])
            return

        if self.is_tenant_switch(port['switch']['tags']):
            msg = 'port |%s| is a tenant network port, skipping for now'
            LOG.warn(msg, port['uuid'])
            return

        if not port['rxtx_base']:
            msg = "port |%s| can't fix queue with no switch qos_pool"
            LOG.error(msg, port['uuid'])

        queue = {'display_name': port['qos_pool']['uuid'],
                 'vmid': port['instance_id'],
                 'rxtx_cap': int(port['rxtx_base'] * port['rxtx_factor'])}

        LOG.action('creating queue: |%s|', queue)
        LOG.action('associating port |%s| with queue |%s|',
                   port['uuid'], queue)

        if action == 'fix':
            # create queue
            queue = self.nvp.create_queue(**queue)
            print queue

            # assign queue to port
            port = self.nvp.port_update_queue(port, queue['uuid'])
            print port

    # done
    def get_no_queue_ports(self):
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        ports = self.nvp.get_ports(relations, limit=10)

        bad_port_list = []
        for port in ports:
            # pull out relations for easy access
            queue = port['_relations']['LogicalQueueConfig']
            status = port['_relations']['LogicalPortStatus']
            attachment = port['_relations']['LogicalPortAttachment']
            switch = port['_relations']['LogicalSwitchConfig']

            switch_dict = {'uuid': status['lswitch']['uuid'],
                           'name': switch['display_name'],
                           'tags': switch['tags'],
                           'transport_zone_uuid': \
                                   switch['transport_zones'][0]['zone_uuid']}

            port_dict = {'uuid': port.get('uuid', ''),
                         'switch': switch_dict,
                         'switch_name': switch['display_name'],
                         'vif_uuid': attachment.get('vif_uuid', ''),
                         'rxtx_cap': queue.get('max_bandwidth_rate', '')}

            qp = self.get_qos_pool(port_dict)
            port_dict['qos_pool'] = qp
            port_dict['rxtx_base'] = qp['max_bandwidth_rate'] if qp else ''

            # get the instance and its flavor rxtx_factor
            get_instance = self.get_instance_by_port
            instance = get_instance(port_dict, join_flavor=True) or {}
            port_dict['instance_id'] = instance.get('uuid', '')
            port_dict['instance_flavor'] = instance.get('instance_type_id', '')
            port_dict['rxtx_factor'] = instance.get('rxtx_factor', '')

            if not queue:
#            if port_dict['lswitch_name'] not in ('public', 'private'):
                bad_port_list.append(port_dict)

        return bad_port_list


class NVP(object):
    ALL_RELATIONS = ['LogicalPortStatus', 'LogicalPortAttachment',
                     'LogicalQueueConfig', 'LogicalSwitchConfig',
                     'LogicalSwitchStatus',
                     'TransportNodeInterfaceStatus',
                     'VirtualInterfaceConfig']

    def __init__(self, url, username, password):
        self.connection = aiclib.nvp.Connection(url, username=username,
                                                     password=password)

        # small memory cache to prevent multiple lookups
        self.qos_pools_by_id = {}
        self.qos_pools_by_name = {}
        self.transport_zones = {}

    @classmethod
    def _check_relations(cls, relations):
        for relation in relations:
            if relation not in cls.ALL_RELATIONS:
                raise Exception('Bad relation requested: %s' % relation)

    #################### PORTS ################################################

    def delete_port(self, port):
        self.connection.lswitch_port(port['lswitch_uuid'],
                                     port['uuid']).delete()

    def get_ports(self, relations=None, limit=None):
        query = self.connection.lswitch_port('*').query()

        # append length to query
        if limit:
            query = query.length(limit)

        if relations:
            # handle relations
            self._check_relations(relations)
            query = query.relations(relations)

        return IterableQuery(query, limit)

    def port_update_queue(self, port, queue_id):
        port = self.connection.lswitch_port(port['switch']['uuid'],
                                            port['uuid'])
        port.qosuuid(queue_id)
        return port.update()

    #################### SWITCHES #############################################

    def get_switch_by_id(self, id):
        return self.connection.lswitch(uuid=id).read()

    def get_switches(self, limit=None):
        query = self.connection.lswitch().query()
        return IterableQuery(query, limit)

    #################### QUEUES ############################################

    def get_queue_by_id(self, id):
        return self.connection.qos(uuid=id).read()

    def get_queues(self, limit=None):
        query = self.connection.qos().query()
        return IterableQuery(query, limit)

    def create_queue(self, display_name, vmid, rxtx_cap):
        queue = self.connection.qos()
        queue.display_name(display_name)
        queue.tags({'scope': 'vmid',
                    'tag': vmid})
        queue.maxbw_rate(rxtx_cap)
        return queue.create()

    def delete_queue(self, id):
        self.connection.qos(id).delete()

    #################### QOS POOLS ############################################
    # a qos pool is actually a queue but these 2 are special

    def get_qos_pool_by_id(self, id):
        if self.qos_pools_by_id.get(id):
            return self.qos_pools_by_id[id]
        pool = self.connection.qos(uuid=id).read()
        self.qos_pools_by_id[id] = pool
        return pool

    def get_qos_pool_by_name(self, name):
        if self.qos_pools_by_name.get(name):
            return self.qos_pools_by_name[name]
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
        zone = self.connection.zone(uuid=id).read()
        self.transport_zones[id] = zone
        return zone


class MysqlJsonBridgeEndpoint(object):
    def run_query(self, sql):
        payload = {'sql': sql}
        r = self.session.post(self.url, data=payload,
                              verify=False, auth=self.auth)
        return r.json

    def first_result(self, result):
        try:
            return result['result'][0]
        except (IndexError, KeyError):
            return None


class Melange(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()

    def get_interface_by_id(self, id):
        result = self.run_query('select * from interfaces where id="%s"' % id)
        return self.first_result(result)


class Nova(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()

    def get_instance_by_id(self, id, join_flavor=False):
        if join_flavor:
            sql = ('select * from instances left join instance_types '
                   'on instances.instance_type_id=instance_types.id '
                   'where uuid="%s"')
        else:
            sql = 'select * from instances where uuid="%s"'
        result = self.run_query(sql % id)
        return self.first_result(result)
