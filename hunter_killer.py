import aiclib
import logging
import requests
from requests.auth import HTTPBasicAuth
from utils import IterableQuery


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


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
    def _repair_switch_qos_pool(self, switch):
        LOG.action('repairing switch |%s| qos_pool', switch['uuid'])
#        tags = switch['tags']
#        qos_pool = {'scope': 'qos_pool',
#                    'tag': qos_pool_id}
        zone_id = switch['transport_zones'][0]['zone_uuid']
        zone = self.nvp.get_transport_zone_by_id(zone_id)
        zone_name = zone['display_name']
        print zone

    def _get_switch_qos_pool(self, switch):
        tags = switch['tags']
        for tag in tags:
            if tag['scope'] == 'qos_pool':
                qos_pool_id = tag['tag']
                return self.nvp.get_qos_pool_by_id(qos_pool_id)
        LOG.error('switch |%s| |%s| does not have a qos pool!' \
                % (switch['uuid'], switch['display_name']))
        self._repair_switch_qos_pool(switch)
        return None

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

            # get roxtx_base from qos pool
            qos_pool = self._get_switch_qos_pool(switch)
            print qos_pool
            rxtx_base = qos_pool['max_bandwidth_rate'] if qos_pool else ''

            port_dict = {'uuid': port.get('uuid', ''),
                         'lswitch_uuid': status['lswitch']['uuid'],
                         'lswitch_name': switch['display_name'],
                         'lswitch_tags': switch['tags'],
                         'vif_uuid': attachment.get('vif_uuid', ''),
                         'queue': queue.get('max_bandwidth_rate', ''),
                         'rxtx_base': rxtx_base}

            # get the instance and its flavor rxtx_factor
            get_instance = self.get_instance_by_port
            instance = get_instance(port_dict, join_flavor=True) or {}
            port_dict['instance_id'] = instance.get('uuid', '')
            port_dict['instance_flavor'] = instance.get('instance_type_id', '')
            port_dict['rxtx_factor'] = instance.get('rxtx_factor', '')

#            if not port_dict['queue'] or True:
            if port_dict['lswitch_name'] not in ('public', 'private'):
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
        self.qos_pools = {}
        self.transport_zones = {}

    @classmethod
    def _check_relations(cls, relations):
        for relation in relations:
            if relation not in cls.ALL_RELATIONS:
                raise Exception('Bad relation requested: %s' % relation)

    def delete_port(self, port):
        query = self.connection.lswitch_port(port['lswitch_uuid'],
                                             port['uuid']).delete()
        return query

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

    def get_switch_by_id(self, id):
        return self.connection.lswitch(uuid=id).read()

    def get_switches(self, limit=None):
        query = self.connection.lswitch().query()
        return IterableQuery(query, limit)

    def get_qos_pool_by_id(self, id):
        # a qos pool is actually a queue but these 2 are special
        if self.qos_pools.get(id):
            return self.qos_pools[id]
        LOG.info('calling for qos pool')
        pool = self.connection.qos(uuid=id).read()
        self.qos_pools[id] = pool
        return pool

    def get_queue_by_id(self, id):
        LOG.info('calling for queue |%s|', id)
        return self.connection.qos(uuid=id).read()

    def get_queues(self, limit=None):
        query = self.connection.qos().query()
        return IterableQuery(query, limit)

    def get_transport_zone_by_id(self, id):
        if self.transport_zones.get(id):
            return self.transport_zones[id]
        LOG.info('calling for transport zone |%s|', id)
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
        LOG.info('get melange interface |%s|', id)
        result = self.run_query('select * from interfaces where id="%s"' % id)
        return self.first_result(result)


class Nova(MysqlJsonBridgeEndpoint):
    def __init__(self, url, username, password):
        self.url = url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.session()

    def get_instance_by_id(self, id, join_flavor=False):
        LOG.info('get nova instance |%s|', id)
        if join_flavor:
            sql = ('select * from instances left join instance_types '
                   'on instances.instance_type_id=instance_types.id '
                   'where uuid="%s"')
        else:
            sql = 'select * from instances where uuid="%s"'
        result = self.run_query(sql % id)
        return self.first_result(result)
