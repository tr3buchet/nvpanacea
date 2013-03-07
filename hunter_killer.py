import aiclib
from requests.auth import HTTPBasicAuth
from utils import IterableQuery
import logging
import requests
import sys
import time
from datetime import timedelta


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

    def get_instance_by_port(self, port, join_flavor=False):
        instance_id = port['vmid']

        # vmid tag wasn't on port, check the queue
        if not instance_id:
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
                # port went away in the mean time
                pass

    def delete_queue(self, queue):
        LOG.action('delete queue |%s|', queue)
        if self.action == 'fix':
            try:
                return self.nvp.delete_queue(queue)
            except aiclib.nvp.ResourceNotFound:
                # queue went away in the mean time
                pass

#    def get_group_from_iter(self, iterable, number):
#        args = [iter(iterable)] * number
#        return izip_longest(*args)

#        for port_group in izip_longest(*([iter(ports)] * 10)):

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

    def port_add_vmid(self, port, vmid):
        LOG.action('adding vmid |%s| to port |%s| tag', vmid, port['uuid'])
        if self.action == 'fix':
            self.nvp.port_add_vmid(port, vmid)
        else:
            # in fixnoop, we need to "associate" the queue for similar
            # behavior to what happens in fix mode
            port['vmid'] = vmid

    def add_port_to_tree(self, port, tree):
        if port['vmid']:
            instance_id = port['vmid']
        elif port['queue'].get('vmid'):
            instance_id = port['queue']['vmid']
        elif port['instance'].get('uuid'):
            instance_id = port['instance']['uuid']
        else:
            instance_id = 'unknown'

        if instance_id in tree:
            tree[instance_id]['ports'].append(port)
        else:
            tree[instance_id] = {'ports': [port]}

    def populate_tree(self, nvp_ports, type):
        tree = {}
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
                    'link_status_up': status['link_status_up'],
                    'fabric_status_up': status['fabric_status_up'],
                    'vif_uuid': attachment.get('vif_uuid', ''),
                    'isolated': self.is_isolated_switch(switch),
                    'public': self.is_public_switch(switch),
                    'snet': self.is_snet_switch(switch),
                    'vmid': self.get_tag(nvp_port, 'vmid') or ''}

            qp = self.get_qos_pool(port)
            qos_pool = {'uuid': qp['uuid'],
                        'max_bandwidth_rate':
                            qp['max_bandwidth_rate'] if qp else ''}
            port['qos_pool'] = qos_pool

            port['instance'] = {}
            if type == 'orphan_ports':
                if not (port['link_status_up'] or port['fabric_status_up']):
                    # only care about ports with link and fabric status down
                    # get instance and add to tree
                    # NOTE: only link/fabric down ports will be in tree
                    # in cases where an exception is raised getting an instance
                    # the port will not be added to the tree
                    try:
                        port['instance'] = self.get_instance_by_port(port)
                        port['instance'] = port['instance'] or {}

                        #### pass in orphan to add_port_to tree to only use
                        #### port['instance']
                        ####

                        self.add_port_to_tree(port, tree)
                    except Exception as e:
                        LOG.error(e)
            elif type == 'no_queue_ports':
                if not port['queue']:
                    # only need instance for ports without a queue
                    # otherwise we can just use the queue (more efficient)
                    # NOTE: all ports will be in the tree for queue repair
                    # in cases where an exception is raised, the port will not
                    # have an instance and will be ignored in queue repair
                    try:
                        get_inst = self.get_instance_by_port
                        instance = get_inst(port, join_flavor=True) or {}
                        port['instance'] = instance
                    except Exception as e:
                        LOG.error(e)
                self.add_port_to_tree(port, tree)
            elif type == 'no_vmids':
                if not port['vmid']:
                    if not port['queue']:
                        # only need instance for ports without a queue
                        # otherwise we can just use the queue (more efficient)
                        # NOTE: only ports without vmid will be in the tree
                        # ports without any kind of instance id will be in the
                        # but will be ignored by add_vmid
                        try:
                            port['instance'] = self.get_instance_by_port(port)
                            port['instance'] = port['instance'] or {}
                        except Exception as e:
                            LOG.error(e)
                    self.add_port_to_tree(port, tree)

            sys.stdout.write('.')
            sys.stdout.flush()
        print
        return tree

    def port_manoeuvre(self, type, limit=None):
        self.start_time = time.time()
        self.queues_checked = 0
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        nvp_ports = self.nvp.get_ports(relations, limit=limit)

        print ('populating tree, check out loglevel INFO if you want to watch,'
              ' a . is a port')

        tree = self.populate_tree(nvp_ports, type)
        if type == 'orphan_ports':
            self.fix_orphan_ports(tree)
        elif type == 'no_queue_ports':
            self.fix_no_queue_ports(tree)
        elif type == 'no_vmids':
            self.add_vmids(tree)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))

    def get_associated_queues(self, nvp_ports):
        # returns a list of queue uuids found in port associations
        associated_queues = []
        for nvp_port in nvp_ports:
            self.ports_checked += 1
            nvp_queue = nvp_port['_relations']['LogicalQueueConfig']
            if nvp_queue and nvp_queue['uuid'] not in associated_queues:
                associated_queues.append(nvp_queue['uuid'])

        return associated_queues

    def queue_manoeuvre(self, type):
        self.start_time = time.time()
        port_relations = ('LogicalQueueConfig', )
        all_queues = [q['uuid'] for q in self.nvp.get_queues()
                                if self.get_tag(q, 'qos_pool') is None]
        self.queues_checked = len(all_queues)
        nvp_ports = self.nvp.get_ports(port_relations)
        associated_queues = self.get_associated_queues(nvp_ports)
        self.fix_orphan_queues(all_queues, associated_queues)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))

    def fix_orphan_queues(self, all_queues, associated_queues):
        orphans = 0
        for queue in all_queues:
            if queue not in associated_queues:
                orphans += 1
                self.delete_queue(queue)
        print 'orphans fixed:', orphans

    def add_vmids(self, tree):
        no_vmids = 0
        for instance_id, values in tree.iteritems():
            if instance_id == 'unknown':
                # these are the ports with unknown instance, probably orphan
                continue
            for port in values['ports']:
                if not port['vmid']:
                    no_vmids += 1
                    self.port_add_vmid(port, instance_id)
        print 'vmids added:', no_vmids

    def fix_no_queue_ports(self, tree):
        no_queues = 0
        for instance_id, values in tree.iteritems():
            if instance_id == 'unknown':
                # these are the ports that had queues, instance wasn't needed
                continue
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
                    other_ports = [p for p in tree[instance_id]['ports']
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
        print 'queues fixed:', no_queues

    def fix_orphan_ports(self, tree):
        orphans = 0
        for instance_id, values in tree.iteritems():
            ports = values['ports']
            if instance_id == 'unknown':
                # ports with no instance are orphans
                for port in ports:
                    orphans += 1
                    LOG.action('found port |%s| w/no instance', port['uuid'])
                    self.delete_port(port)
            else:
                # ports with deleted instances are orphans
                for port in ports:
                    if port['instance'].get('vm_state') == 'deleted':
                        orphans += 1
                        msg = 'found port |%s| w/instance |%s| in state |%s|'
                        LOG.action(msg, port['uuid'], instance_id,
                                        port['instance']['vm_state'])
                        self.delete_port(port)
        print 'orphans fixed:', orphans

    def print_calls_made(self):
        msg = ('%s ports processed\n%s queues processed\n%s calls to nvp\n'
               '%s calls to melange\n%s calls to nova\n'
               'time taken %s')
        print msg % (self.ports_checked, self.queues_checked, self.nvp.calls,
                     self.melange.calls, self.nova.calls, self.time_taken)


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
        self.connection.lswitch_port(port['switch']['uuid'],
                                     port['uuid']).delete()

    def get_port(self, port):
        # get an nvp port form our local dict object
        self.calls += 1
        query = self.connection.lswitch_port(port['switch']['uuid'],
                                             port['uuid']).query().results()
        try:
            return query['results'][0]
        except (KeyError, IndexError):
            return None

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

    def port_add_vmid(self, port, vmid):
        self.calls += 1
        nvp_port = self.get_port(port)

        new_tag = {'scope': 'vmid',
                   'tag': vmid}

        # get existing tags, if any, and append vmid tag
        if 'tags' in nvp_port:
            tags = nvp_port['tags']
            tags.append(new_tag)
        else:
            tags = [new_tag]

        port_query_obj = self.connection.lswitch_port(port['switch']['uuid'],
                                                      port['uuid'])
        port_query_obj.tags(tags)
        return port_query_obj.update()

    #################### SWITCHES #############################################

    def get_switch_by_id(self, id):
        self.calls += 1
        return self.connection.lswitch(uuid=id).read()

    def get_switches(self, limit=None):
        query = self.connection.lswitch().query()
        return IterableQuery(self, query, limit)

    def switch_update_tag(self, switch, tag_scope, tag_value):
        nvp_switch = self.get_switch_by_id(switch['uuid'])
        new_tag = {'scope': tag_scope,
                   'tag': tag_value}

        # get existing tags, if any, and append vmid tag
        if 'tags' in nvp_switch:
            tags = nvp_switch['tags']
            # update tag if it exists
            for tag in tags:
                if tag['scope'] == tag_scope:
                    LOG.action('changing %s tag %s to %s',
                               switch['name'], tag, new_tag)
                    tag['tag'] = tag_value
                    break
            else:
                # didn't find tag with scope
                # append this tag to existing tags
                print 'adding tag %s' % new_tag
                tags.append(new_tag)
        else:
            # create net tag list since there are none
            print 'adding tag %s' % new_tag
            tags = [new_tag]

        switch_query_obj = self.connection.lswitch(switch['uuid'])

        switch_query_obj.tags(tags)
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
