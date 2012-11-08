import aiclib
import logging
import requests
from requests.auth import HTTPBasicAuth
from utils import IterableQuery


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


zone_qos_pool_map = {'public': 'pub_base_rate',
                     'private': 'snet_base_rate'}


class HunterKiller(object):
    def __init__(self, action,
                 nvp_url, nvp_username, nvp_password,
                 nova_url, nova_username, nova_password,
                 melange_url, melange_username, melange_password):
        self.action = action
        self.nvp = NVP(nvp_url, nvp_username, nvp_password)
        self.nova = Nova(nova_url, nova_username, nova_password)
        self.melange = Melange(melange_url, melange_username, melange_password)
        self.ports_checked = 0
        self.tree = {}

    def get_instance_by_port(self, port, join_flavor=False):
        instance_id = port['queue'].get('vmid')

        # get instance_id from melange if we don't already have it
        if not instance_id:
            interface = self.melange.get_interface_by_id(port['vif_uuid']) \
                        if port['vif_uuid'] else None
            # if we got an interface back, grab it's device_id
            if interface:
                instance_id = interface['device_id']

        # if we ended up with an instance_id, attempt to get instance
        if instance_id:
            return self.nova.get_instance_by_id(instance_id,
                                                join_flavor)

    def delete_port(self, port):
        LOG.action('delete port |%s|', port['uuid'])
        if self.action == 'fix':
            try:
                return self.nvp.delete_port(port)
            except aiclib.nvp.ResourceNotFound:
                pass

#    def get_group_from_iter(self, iterable, number):
#        args = [iter(iterable)] * number
#        return izip_longest(*args)

#        for port_group in izip_longest(*([iter(ports)] * 10)):

    def orphaned_port(self, nvp_port):
        # pull out relations for easy access
        queue = nvp_port['_relations']['LogicalQueueConfig']
        status = nvp_port['_relations']['LogicalPortStatus']
        attachment = nvp_port['_relations']['LogicalPortAttachment']
        lstatus = 'up' if status['link_status_up'] else 'down'
        fstatus = 'up' if status['fabric_status_up'] else 'down'
        port = {'uuid': nvp_port.get('uuid', ''),
                'lswitch_uuid': status['lswitch']['uuid'],
                'vif_uuid': attachment.get('vif_uuid', ''),
                'link_status': lstatus,
                'fabric_status': fstatus,
                'instance_id': self.get_tag(queue, 'vmid') or ''}

        # get the instance
        instance = self.get_instance_by_port(port)
        if instance:
            port['instance_id'] = instance['uuid']
            port['instance_state'] = instance['vm_state']
            port['instance_terminated_at'] = \
                    instance['terminated_at'] or ''
        else:
            port['instance_id'] = None
            port['instance_state'] = None
            port['instance_terminated_at'] = None

        # only delete ports with no instance
        # TODO: only return ports if instance_terminated_at > x hours
        if not instance or instance['vm_state'] == 'deleted':
            if self.action == 'list':
                print
            elif self.action in ('fix', 'fixnoop'):
                self.delete_port(port)

    def get_tag(self, obj, tag_name):
        if 'tags' in obj:
            for tag in obj['tags']:
                if tag['scope'] == tag_name:
                    return tag['tag']

    def is_isolated_switch(self, switch):
        # if switch  has a qos pool, it is not isolated
        qos_pool_id = self.get_tag(switch, 'qos_pool')
        return False if qos_pool_id else True

    def is_public_switch(self, switch):
        zone_id = switch['transport_zone_uuid']
        zone = self.nvp.get_transport_zone_by_id(zone_id)
        zone_name = zone['display_name']
        return zone_name == 'public'

    def is_snet_switch(self, switch):
        zone_id = switch['transport_zone_uuid']
        zone = self.nvp.get_transport_zone_by_id(zone_id)
        zone_name = zone['display_name']
        return zone_name == 'private'

    def get_qos_pool_from_switch(self, switch):
        qos_pool_id = self.get_tag(switch, 'qos_pool')
        if qos_pool_id:
            return self.nvp.get_qos_pool_by_id(qos_pool_id)

    def get_qos_pool_from_transport_zone_map(self, zone_id):
        zone = self.nvp.get_transport_zone_by_id(zone_id)
        zone_name = zone['display_name']
        qos_pool_name = zone_qos_pool_map[zone_name]
        return self.nvp.get_qos_pool_by_name(qos_pool_name)

    def get_qos_pool(self, port):
        qos_pool = self.get_qos_pool_from_switch(port['switch'])
        if qos_pool:
            return qos_pool

        msg = 'port |%s| switch |%s||%s| does not have a qos pool!'
        LOG.warn(msg, port['uuid'], port['switch']['uuid'],
                 port['switch']['name'])

        # lswitch didn't have a qos_pool, have to use transport zone
        zone_id = port['switch']['transport_zone_uuid']
        qos_pool = self.get_qos_pool_from_transport_zone_map(zone_id)
        if qos_pool:
            return qos_pool

        msg = "qos pool couldn't be found using transport zone map either!"
        LOG.error(msg, port['uuid'], port['switch']['uuid'],
                  port['switch']['name'])

    def create_queue(self, port):
        LOG.action('create queue for port |%s|', port['uuid'])
        if port['queue']:
            LOG.warn('port |%s| already has a queue!', port['uuid'])
            return

        queue = {'display_name': port['qos_pool']['uuid'],
                 'vmid': port['instance']['uuid']}
        try:
            rxtx_factor = port['instance']['rxtx_factor']
            rxtx_base = port['qos_pool']['max_bandwidth_rate']
            queue['max_bandwidth_rate'] = int(rxtx_base) * int(rxtx_factor)
        except ValueError:
            LOG.error('rxtx_cap calculation failed. base: |%s|, factor: |%s|',
                      port['rxtx_base'], port['rxtx_factor'])
            return

        LOG.action('creating queue: |%s|', queue)
        if self.action == 'fix':
            nvp_queue = self.nvp.create_queue(**queue)
            if nvp_queue:
                return {'uuid': nvp_queue['uuid'],
                        'max_bandwidth_rate': nvp_queue['max_bandwidth_rate'],
                        'vmid': self.get_tag(nvp_queue, 'vmid') or ''}
        # return a fake uuid for noop mode
        queue['uuid'] = 'fake'
        return queue

    def associate_queue(self, port, queue):
        LOG.action('associating port |%s| with queue |%s|',
                   port['uuid'], queue['uuid'])
        if self.action == 'fix':
            try:
                self.nvp.port_update_queue(port, queue['uuid'])
                port['queue'] = queue
            except aiclib.nvp.ResourceNotFound:
                LOG.error('port was not associated!')
                # TODO: delete the queue we just made
        else:
            # in fixnoop, we need to "associate" the queue for similar
            # behavior to what happens in fix mode
            port['queue'] = queue

    def add_port_to_tree(self, port):
        if port['queue'].get('vmid'):
            instance_id = port['queue']['vmid']
        elif port['instance'].get('uuid'):
            instance_id = port['instance']['uuid']
        else:
            return

        if instance_id in self.tree:
            self.tree[instance_id]['ports'].append(port)
        else:
            self.tree[instance_id] = {'ports': [port]}

    def populate_tree(self, nvp_ports, populate_instance=False):
        for nvp_port in nvp_ports:
            self.ports_checked += 1
            nvp_queue = nvp_port['_relations']['LogicalQueueConfig']
            status = nvp_port['_relations']['LogicalPortStatus']
            attachment = nvp_port['_relations']['LogicalPortAttachment']
            nvp_switch = nvp_port['_relations']['LogicalSwitchConfig']
            LOG.info('populating port |%s|', nvp_port['uuid'])

            switch = {'uuid': status['lswitch']['uuid'],
                      'name': nvp_switch['display_name'],
                      'tags': nvp_switch['tags'],
                      'transport_zone_uuid': \
                          nvp_switch['transport_zones'][0]['zone_uuid']}

            queue = {}
            if nvp_queue:
                queue = {'uuid': nvp_queue['uuid'],
                         'max_bandwidth_rate': nvp_queue['max_bandwidth_rate'],
                         'vmid': self.get_tag(nvp_queue, 'vmid') or ''}

            port = {'uuid': nvp_port.get('uuid', ''),
                    'switch': switch,
                    'queue': queue,
                    'vif_uuid': attachment.get('vif_uuid', ''),
                    'isolated': self.is_isolated_switch(switch),
                    'public': self.is_public_switch(switch),
                    'snet': self.is_snet_switch(switch)}

            qp = self.get_qos_pool(port)
            qos_pool = {'uuid': qp['uuid'],
                        'max_bandwidth_rate':
                            qp['max_bandwidth_rate'] if qp else ''}
            port['qos_pool'] = qos_pool

            port['instance'] = {}
            if populate_instance or not port['queue']:
                try:
                    get_inst = self.get_instance_by_port
                    instance = get_inst(port, join_flavor=True) or {}
                    port['instance'] = instance
                except:
                    pass

            self.add_port_to_tree(port)

    def port_manoeuvre(self, type):
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        nvp_ports = self.nvp.get_ports(relations)

        print 'populating tree, check out INFO if you want to watch'
        self.populate_tree(nvp_ports)

        if type == 'orphan_ports':
            handle_ports = self.orphan_port
        elif type == 'no_queue_ports':
            handle_ports = self.no_queue_ports

        handle_ports()

    def no_queue_ports(self):
        no_queues = 0
        for instance_id, values in self.tree.iteritems():
            ports = values['ports']
            for port in ports:
                if port['queue']:
                    continue
                no_queues += 1

                if port['public']:
                    msg = 'creating queue for public port |%s|'
                    LOG.action(msg, port['uuid'])
                    queue = self.create_queue(port)
                    self.associate_queue(port, queue)
                elif port['snet'] or port['isolated']:
                    # other ports will be snet or isolated nw w/ queue
                    other_ports = [p for p in self.tree[instance_id]['ports']
                                   if p['queue'] and
                                   (p['snet'] or p['isolated'])]
                    if other_ports:
                        msg = ('associating queue for snet/isolated port |%s| '
                               'with snet/isolated port |%s| queue')
                        other_port = other_ports[0]
                        LOG.action(msg, port['uuid'], other_port['uuid'])
                        self.associate_queue(port, other_port['queue'])
                    else:
                        msg = 'creating queue for snet/isolated port |%s|'
                        LOG.action(msg, port['uuid'])
                        queue = self.create_queue(port)
                        self.associate_queue(port, queue)
        print 'no queues ', no_queues
        return

        nvp_port = {}
        # pull out relations for easy access
        queue = nvp_port['_relations']['LogicalQueueConfig']
        status = nvp_port['_relations']['LogicalPortStatus']
        attachment = nvp_port['_relations']['LogicalPortAttachment']
        nvp_switch = nvp_port['_relations']['LogicalSwitchConfig']
        LOG.info('testing port |%s|', nvp_port['uuid'])

        if not queue:
            LOG.info('port |%s| had no queue, getting instance',
                     nvp_port['uuid'])
            switch = {'uuid': status['lswitch']['uuid'],
                      'name': nvp_switch['display_name'],
                      'tags': nvp_switch['tags'],
                      'transport_zone_uuid': \
                          nvp_switch['transport_zones'][0]['zone_uuid']}

            port = {'uuid': nvp_port.get('uuid', ''),
                    'switch': switch,
                    'vif_uuid': attachment.get('vif_uuid', ''),
                    'rxtx_cap': queue.get('max_bandwidth_rate', ''),
                    'instance_id': self.get_tag(queue, 'vmid') or '',
                    'isolated': self.is_isolated_switch(switch)}

            # take requested action
            if self.action == 'list':
                msg = 'port |%s| has no queue on switch |%s||%s|'
                print msg % (port['uuid'], port['switch']['name'],
                             port['switch']['uuid'])
            elif self.action in ('fix', 'fixnoop'):
                # grab bandwidth from qos pool
                qp = self.get_qos_pool(port)
                port['qos_pool_uuid'] = qp['uuid']
                port['rxtx_base'] = \
                        qp['max_bandwidth_rate'] if qp else ''

                try:
                    # get the instance and its flavor rxtx_factor
                    get_inst = self.get_instance_by_port
                    instance = get_inst(port, join_flavor=True) or {}
                    LOG.info('found instance |%s|',
                             instance.get('uuid', ''))
                    port['instance_id'] = instance.get('uuid', '')
                    port['instance_flavor'] = \
                                      instance.get('instance_type_id', '')
                    port['rxtx_factor'] = \
                                      instance.get('rxtx_factor', '')

                except:
                    LOG.error('error getting instance, '
                              'skipping repair phase')
                    return

                self.repair_port_queue(port)

    def calls_made(self):
        msg = ('%s ports processed\n%s calls to nvp\n'
               '%s calls to melange\n%s calls to nova')
        return msg % (self.ports_checked, self.nvp.calls,
                      self.melange.calls, self.nova.calls)


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

    #################### PORTS ################################################

    def delete_port(self, port):
        self.calls += 1
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

        return IterableQuery(self, query, limit)

    def port_update_queue(self, port, queue_id):
        self.calls += 1
        port = self.connection.lswitch_port(port['switch']['uuid'],
                                            port['uuid'])
        port.qosuuid(queue_id)
        return port.update()

    #################### SWITCHES #############################################

    def get_switch_by_id(self, id):
        self.calls += 1
        return self.connection.lswitch(uuid=id).read()

    def get_switches(self, limit=None):
        query = self.connection.lswitch().query()
        return IterableQuery(self, query, limit)

    #################### QUEUES ############################################

    def get_queues(self, limit=None):
        query = self.connection.qos().query()
        return IterableQuery(self, query, limit)

    def create_queue(self, display_name, vmid, max_bandwidth_rate):
        self.calls += 1
        queue = self.connection.qos()
        queue.display_name(display_name)
        queue.tags({'scope': 'vmid',
                    'tag': vmid})
        queue.maxbw_rate(max_bandwidth_rate)
        return queue.create()

    def delete_queue(self, id):
        self.calls += 1
        self.connection.qos(id).delete()

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
        return r.json

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
            sql = 'select %s from instances where uuid="%s"'
        result = self.run_query(sql % (','.join(select_list), id))
        return self.first_result(result)


class Port(dict):
    def __repr__(self):
        return self.__class__.__name__ + '(' + dict.__repr__(self) + ')'
