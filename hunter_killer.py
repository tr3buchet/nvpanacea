import logging
import sys
import time
from datetime import timedelta
import utils
from pprint import pprint
import json
import netaddr
# from gevent.pool import Pool

import querylib

import pymysql


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(25, s, *args, **kwargs)
LOG.output = lambda s, *args, **kwargs: LOG.log(35, s, *args, **kwargs)


zone_qos_pool_map = {'public': 'pub_base_rate',
                     'private': 'snet_base_rate'}

# GEVENT_THREADS = 10


class Summary(object):
    def __init__(self):

        # timer
        self.timer = False
        self.start = None
        self.stop = None

        # general
        self.general = False
        self.good = []
        self.bad = []
        self.fixed = []

        # port counts (link status, fabric status)
        self.port_counts = False
        self.no_vmid_tag = []
        self.instance_states = {}
        self.has_neutron_port = []
        self.no_neutron_port = []
        self.has_instance = []
        self.no_instance = []

        # port types (isolated, public, snet)
        self.port_types = False
        self.ports_public = 0
        self.ports_snet = 0
        self.ports_isolated = 0
        self.ports_orphaned = 0

    def start_timer(self):
        self.timer = True
        self.start = time.time()

    def stop_timer(self):
        self.stop = time.time()

    @property
    def time_taken(self):
        if self.stop:
            return timedelta(seconds=(self.stop - self.start))
        return timedelta(seconds=(time.time() - self.start))

    def _append(self, lijst, value):
        self.general = True
        lijst.append(value)

    def addgood(self, thing_uuid):
        self._append(self.good, thing_uuid)

    def addbad(self, thing_uuid):
        self._append(self.bad, thing_uuid)

    def addfixed(self, thing_uuid):
        self._append(self.fixed, thing_uuid)

    def update_port_counts(self, port):
        self.port_counts = True
        if port['vmid'] is None:
            self.no_vmid_tag.append(port['uuid'])
            return
        if port['instance']:
            self.has_instance.append(port['uuid'])
        else:
            self.no_instance.append(port['uuid'])
        if port['neutron_port']:
            self.has_neutron_port.append(port['uuid'])
        else:
            self.no_neutron_port.append(port['uuid'])

        if port['instance'].get('vm_state') in self.instance_states:
            self.instance_states[port['instance'].get('vm_state')] += 1
        else:
            self.instance_states[port['instance'].get('vm_state')] = 1

    def __str__(self):
        s = '######## SUMMARY ########\n'
        if self.timer:
            s += ('time taken: %s\n' % self.time_taken)
        if self.general:
            if self.no_vmid_tag:
                s += '\ntotal: %d\n' % (len(self.good) +
                                        len(self.bad) + len(self.no_vmid_tag))
            else:
                s += '\ntotal: %d\n' % (len(self.good) + len(self.bad))
            s += 'good: %d\n' % len(self.good)
            s += 'bad: %d\n' % len(self.bad)
            s += 'fixed: %d\n' % len(self.fixed)

        if self.port_counts:
            s += '\nports:\n'
            s += '  no vmid tag: %d\n' % len(self.no_vmid_tag)
            if self.instance_states:
                s += '  instance status counts:\n'
                for k, v in self.instance_states.iteritems():
                    s += '    %s: %d\n' % (k, v)
            s += '  vm/port counts:\n'
            s += '    has neutron port: %d\n' % len(self.has_neutron_port)
            s += '    no neutron port: %d\n' % len(self.no_neutron_port)
            s += '    has instance: %d\n' % len(self.has_instance)
            s += '    no instance: %d\n' % len(self.no_instance)

        if self.port_types:
            s += '  port type counts:\n'
            s += '    total ports: %d\n' % (self.ports_public +
                                            self.ports_snet +
                                            self.ports_isolated +
                                            self.ports_orphaned)
            s += '    public ports: %d\n' % self.ports_public
            s += '    snet ports: %d\n' % self.ports_snet
            s += '    isolated ports: %d\n' % self.ports_isolated
            s += '    orphaned ports: %d\n' % self.ports_orphaned
        return s


class HunterKiller(object):
    def __init__(self, action,
                 nvp_url, nvp_username, nvp_password,
                 nova_url, nova_username, nova_password,
                 melange_url, melange_username, melange_password,
                 neutron_url, neutron_username, neutron_password,
                 *args, **kwargs):
        self.action = action
        self.nvp = querylib.NVP(nvp_url, nvp_username, nvp_password)
        self.nova = querylib.Nova(nova_url, nova_username, nova_password)
        self.melange = querylib.Melange(melange_url, melange_username,
                                        melange_password)
        self.neutron = querylib.Neutron(neutron_url, neutron_username,
                                        neutron_password)
        self.summary = Summary()

    def execute(self, *args, **kwargs):
        self.process()
        self.display_summary()

    def process(self, *args, **kwargs):
        raise NotImplementedError()

    def print_calls_made(self, ports=None, queues=None):
        msg = ''
        if ports is not None:
            msg = '%s ports processed\n' % ports
        if queues is not None:
            msg += '%s queues_checked\n' % queues

        msg += ('%s calls to nvp\n%s calls to melange\n%s calls to nova\n'
                '%s calls to neutron')
        LOG.output('')
        LOG.output(msg % (self.nvp.calls, self.melange.calls,
                          self.nova.calls, self.neutron.calls))

#        print '\n'
        LOG.output('')
        LOG.output(self.summary)


class HunterKillerPortOps(HunterKiller):
    def get_instance(self, port, instances):
        # NOTE(tr3buchet): only works after nvp port has been port dicted
        instance_id = port['vmid']
        if not instance_id:
            instance_id = port['neutron_port'].get('device_id')

        # NOTE(tr3buchet): from when ports had queues
        # vmid tag wasn't on port, check the queue
        # if not instance_id:
        #     instance_id = port['queue'].get('vmid')

        # NOTE(tr3buchet): from when we used melange
        # get instance_id from melange interfaces if we don't already have it
        # if not instance_id:
        #     interface = interfaces.get(port['vif_uuid']) \
        #         if port['vif_uuid'] else None
        #     # if we found an interface, grab it's device_id
        #     if interface:
        #         instance_id = interface['device_id']

        # if we ended up with an instance_id, see if we have an instance
        # and return it
        if instance_id:
            return instances.get(instance_id) or {}
        return {}

    def get_neutron_port(self, port, neutron_ports):
        # NOTE(tr3buchet): only works after nvp port has been port dicted
        # return neutron_ports.get(port['vif_uuid'], {})
        return neutron_ports.get(port['neutron_port_uuid'], {})

    def is_isolated_switch(self, switch):
        # if switch  has a qos pool, it is not isolated
        return 'qos_pool' not in switch['tags']

    def is_public_switch(self, switch):
        if switch:
            zone_id = switch['transport_zone_uuid']
            zone = self.nvp.get_transport_zone_by_id(zone_id)
            zone_name = zone['display_name']
            return zone_name == 'public'

    def is_snet_switch(self, switch):
        if switch:
            zone_id = switch['transport_zone_uuid']
            zone = self.nvp.get_transport_zone_by_id(zone_id)
            zone_name = zone['display_name']
            return zone_name == 'private'

    def get_qos_pool_from_switch(self, switch):
        qos_pool_id = switch['tags'].get('qos_pool')
        if qos_pool_id:
            return self.nvp.get_qos_pool_by_id(qos_pool_id)

    def get_qos_pool_from_transport_zone_map(self, zone_id):
        zone = self.nvp.get_transport_zone_by_id(zone_id)
        zone_name = zone['display_name']
        qos_pool_name = zone_qos_pool_map[zone_name]
        return self.nvp.get_qos_pool_by_name(qos_pool_name)

    def get_qos_pool(self, switch):
        qos_pool = self.get_qos_pool_from_switch(switch)
        if qos_pool:
            return qos_pool

        msg = 'switch |%s||%s| does not have a qos pool!'
        LOG.info(msg % (switch['uuid'], switch['name']))

        # lswitch didn't have a qos_pool, have to use transport zone
        # read: isolated nw port switch
        zone_id = switch['transport_zone_uuid']
        qos_pool = self.get_qos_pool_from_transport_zone_map(zone_id)
        if qos_pool:
            return qos_pool

        LOG.error('qos pool couldnt be found using transport zone map either!')

    def create_port_dict(self, nvp_port, instances=None, neutron_ports=None):
        LOG.info('populating port |%s|' % nvp_port['uuid'])

        attachment = {}
        nvp_attachment = nvp_port['_relations'].get('LogicalPortAttachment')
        if nvp_attachment:
            attachment = {'vif_uuid': nvp_attachment.get('vif_uuid')}

        queue = {}
        nvp_queue = nvp_port['_relations'].get('LogicalQueueConfig')
        if nvp_queue:
            tags = self.nvp.tags_to_dict(nvp_queue)
            queue = {'uuid': nvp_queue['uuid'],
                     'max_bandwidth_rate': nvp_queue.get('max_bandwidth_rate'),
                     'vmid': tags.get('vmid'),
                     'tags': tags,
                     'ignored': 'ignored_nvpanacea' in tags}

        status = {}
        nvp_status = nvp_port['_relations'].get('LogicalPortStatus')
        if nvp_status:
            status = {'lswitch_uuid': nvp_status['lswitch']['uuid'],
                      'link_status_up': nvp_status['link_status_up'],
                      'fabric_status_up': nvp_status['fabric_status_up']}

        switch = {}
        qos_pool = {}
        nvp_switch = nvp_port['_relations'].get('LogicalSwitchConfig')
        if nvp_switch:
            tags = self.nvp.tags_to_dict(nvp_switch)
            switch = {'uuid': status.get('lswitch_uuid'),
                      'name': nvp_switch['display_name'],
                      'tags': tags,
                      'transport_zone_uuid':
                      nvp_switch['transport_zones'][0]['zone_uuid']}
            qos_pool = self.get_qos_pool(switch) or {}
            qos_pool = {'uuid': qos_pool.get('uuid'),
                        'max_bandwidth_rate':
                        qos_pool.get('max_bandwidth_rate')}

        tags = self.nvp.tags_to_dict(nvp_port)
        # NOTE(tr3buchet): vif_uuid and neutron_port_uuid appear to be the same
        #                  but neutron port seems to be more reliably populated
        port = {'uuid': nvp_port['uuid'],
                'tags': tags,
                'switch': switch,
                'qos_pool': qos_pool,
                'queue': queue,
                'link_status_up': status.get('link_status_up'),
                'fabric_status_up': status.get('fabric_status_up'),
                'vif_uuid': attachment.get('vif_uuid'),
                'isolated': self.is_isolated_switch(switch),
                'public': self.is_public_switch(switch),
                'snet': self.is_snet_switch(switch),
                'vmid': tags.get('vm_id'),
                'neutron_port_uuid': tags.get('neutron_port_id'),
                'neutron_port': {},
                'instance': {}}
        if neutron_ports:
            port['neutron_port'] = self.get_neutron_port(port, neutron_ports)
        if instances:
            port['instance'] = self.get_instance(port, instances)

        return port


class OrphanPorts(HunterKillerPortOps):
    """ deletes orphan ports as they are found """
    def __init__(self, *args, **kwargs):
        super(OrphanPorts, self).__init__(*args, **kwargs)
        self.summary.start_timer()

    def execute(self):
        self.ports_checked = 0
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        # switch_uuid = '408b3d00-fe9d-4b04-8182-516c9dcbc33b'
        nvp_ports = self.nvp.get_ports(relations)
        # nvp_ports = self.nvp.get_ports(relations, switch_uuid=switch_uuid)
        instances = self.nova.get_instances()
        neutron_ports = self.neutron.get_ports(hash_by='id')

        # print what we've managed to get (sanity check)
        msg = ('Found\n'
               '|%s| nvp_ports\n'
               '|%s| intances\n'
               '|%s| neutron_ports\n'
               'ctrl-c in 10 seconds if this doesn\'t look right')
        LOG.output(msg % (len(nvp_ports), len(instances), len(neutron_ports)))
        time.sleep(10)
        msg = ('Walking |%s| ports to find orphans, '
               'check out loglevel INFO if you want to watch. a . is a port')
        LOG.output(msg % len(nvp_ports))

        self.walk_port_list(nvp_ports, instances, neutron_ports)
        self.time_taken = 0
        self.print_calls_made(ports=self.ports_checked)

    def walk_port_list(self, nvp_ports, instances, neutron_ports):
        # walk port list populating port to get the instance
        # if any error is raised getting instance, ignore the port
        # if port is deemed orphan, fix it
        orphans_fixed = 0
        for nvp_port in nvp_ports:
            port = self.create_port_dict(nvp_port, instances, neutron_ports)
            self.ports_checked += 1
            self.summary.update_port_counts(port)
            if port['vmid'] is None:
                # NOTE(tr3buchet): rackconnect johnnie ports don't have vmid
                #                  and totally look orphan so skip entirely
                continue

            if self.is_orphan_port(port):
                self.summary.addbad(port['uuid'])
                sys.stdout.write('o')
                sys.stdout.flush()
                try:
                    self.delete_port(port)
                    self.summary.addfixed(port['uuid'])
                    orphans_fixed += 1
                except Exception as e:
                    LOG.exception(e)
            else:
                self.summary.addgood(port['uuid'])
                sys.stdout.write('.')
                sys.stdout.flush()

    def is_orphan_port(self, port):
        if not port['neutron_port']:
            LOG.warn('found port |%s| w/no neutron port' % port['uuid'])
            return True
        return False

    def delete_port(self, port):
        LOG.action('delete port |%s|' % port['uuid'])
        if self.action == 'fix':
            self.nvp.delete_port(port)


# class RepairQueues(HunterKillerPortOps):
#    """ creates a tree port/queue information and repairs queues.
#
#        can do one of these things:
#        1) create a queue and associate port(s) with it
#        2) associate a port to an existing queue
#        3) update the max_bandwidth_rate on a port's queue
#    """
#    def execute(self):
#        self.ports_checked = 0
#        self.start_time = time.time()
#        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
#                     'LogicalPortAttachment', 'LogicalSwitchConfig')
#        nvp_ports = self.nvp.get_ports(relations)
#        instances = self.nova.get_instances(join_flavor=True)
#        interfaces = self.melange.get_interfaces_hashed_by_id()
#
#        # print what we've managed to get (sanity check)
#        msg = ('Found |%s| ports\n'
#               'Found |%s| intances\n'
#               'Found |%s| interfaces\n'
#               'ctrl-c in 10 seconds if this doesn\'t look right')
#        LOG.output(msg % (len(nvp_ports), len(instances), len(interfaces)))
#        time.sleep(10)
#
#        tree = self.populate_tree(nvp_ports, instances, interfaces)
#        self.fix(tree)
#        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
#        self.print_calls_made(ports=self.ports_checked)
#
#    def populate_tree(self, nvp_ports, instances, interfaces):
#        tree = {}
#        for nvp_port in nvp_ports:
#            self.ports_checked += 1
#            port = self.create_port_dict(nvp_port)
#
#            # repairing a queue requires instance and flavor, so
#            # we need all instances
#            # otherwise we could get vmid queue (more efficient)
#            # NOTE: all ports w/instance will be in the tree for queue repair
#            port['instance'] = self.get_instance_by_port(port, instances,
#                                                         interfaces)
#            # ignore ports with no instance (the orphans)
#            if port['instance']:
#                self.add_port_to_tree(port, tree)
#
#        return tree
#
#    def add_port_to_tree(self, port, tree):
#        instance_id = port['instance']['uuid']
#
#        if instance_id in tree:
#            tree[instance_id].append(port)
#        else:
#            tree[instance_id] = [port]
#
#    def fix(self, tree):
#        queues_repaired = 0
#        no_queues_fixed = 0
#        for instance_id, ports in tree.iteritems():
#            for port in ports:
#                if port['queue']:
#                    # port has queue, make sure it's squared away
#                    if self.ensure_port_queue_bw(port):
#                        queues_repaired += 1
#                else:
#                    # port had no queue, make or associate with one
#                    self.repair_port_queue(tree, instance_id, port)
#                    no_queues_fixed += 1
#        LOG.output('queues repaired:', queues_repaired)
#        LOG.output('no_queues fixed:', no_queues_fixed)
#
#    def repair_port_queue(self, tree, instance_id, port):
#        if port['public']:
#            msg = 'creating queue for public port |%s|'
#            LOG.action(msg % port['uuid'])
#            queue = self.create_queue(port)
#            self.associate_queue(port, queue)
#        elif port['snet'] or port['isolated']:
#            # these ports need to share a queue
#            # other ports will be snet or isolated nw w/ queue
#            other_ports = [p for p in tree[instance_id]
#                           if p['queue'] and
#                           (p['snet'] or p['isolated'])]
#            if other_ports:
#                # found port(s) with a queue to share
#                msg = ('associating queue for snet/isolated port '
#                       '|%s| with snet/isolated port |%s| queue')
#                other_port = other_ports[0]
#                LOG.action(msg % (port['uuid'], other_port['uuid']))
#                self.associate_queue(port, other_port['queue'])
#            else:
#                # no ports had queue to share, create and associate
#                msg = 'creating queue for snet/isolated port |%s|'
#                LOG.action(msg % port['uuid'])
#                queue = self.create_queue(port)
#                self.associate_queue(port, queue)
#
#    def ensure_port_queue_bw(self, port):
#        # returns True if action was taken
#        if port['queue']['ignored']:
#            return False
#        # see if queue max_bw_rate is squared away
#        queue = port['queue']
#        mbwr = queue['max_bandwidth_rate']
#        calc_mbwr = self.calculate_max_bandwidth_rate(port)
#        if mbwr == calc_mbwr:
#            return False
#        else:
#            # it isn't, go ahead and square that away
#            self.update_queue_max_bandwidth_rate(queue, calc_mbwr)
#            return True
#
#    def calculate_max_bandwidth_rate(self, port):
#        try:
#            rxtx_factor = port['instance'].get('rxtx_factor')
#            rxtx_base = port['qos_pool'].get('max_bandwidth_rate')
#            max_bandwidth_rate = int(rxtx_base) * int(rxtx_factor)
#        except (ValueError, TypeError):
#            LOG.error('rxtx_cap calculation failed. '
#                      'base: |%s|, factor: |%s|' %
#                      (rxtx_base, rxtx_factor))
#            return None
#        return max_bandwidth_rate
#
#    def create_queue(self, port):
#        LOG.action('create queue for port |%s|' % port['uuid'])
#        if port['queue']:
#            LOG.error('port |%s| already has a queue!' % port['uuid'])
#            return
#
#        queue = {'display_name': port['qos_pool']['uuid'],
#                 'vmid': port['vmid'] or port['instance']['uuid']}
#
#        max_bandwidth_rate = self.calculate_max_bandwidth_rate(port)
#        if max_bandwidth_rate is None:
#            return
#        queue['max_bandwidth_rate'] = max_bandwidth_rate
#
#        LOG.action('creating queue: |%s|' % queue)
#        if self.action == 'fix':
#            nvp_queue = self.nvp.create_queue(**queue)
#            if nvp_queue:
#                return {'uuid': nvp_queue['uuid'],
#                        'max_bandwidth_rate': nvp_queue['max_bandwidth_rate'],
#                        'vmid': self.nvp.tags_to_dict(nvp_queue).get('vmid')}
#            else:
#                LOG.error('queue creation failed '
#                          'for port |%s|' % port['uuid'])
#        else:
#            # return a fake uuid for noop mode
#            queue['uuid'] = 'fake'
#            return queue
#
#    def associate_queue(self, port, queue):
#        LOG.action('associating port |%s| with queue |%s|' %
#                   (port['uuid'], queue['uuid']))
#        if self.action == 'fix':
#            self.nvp.port_update_queue(port, queue['uuid'])
#            port['queue'] = queue
#        else:
#            # in fixnoop, we need to "associate" the queue for similar
#            # behavior to what happens in fix mode
#            port['queue'] = queue
#
#    def update_queue_max_bandwidth_rate(self, queue, max_bandwidth_rate):
#        msg = 'update max_bandwidth_rate for queue |%s| from |%s| to |%s|'
#        LOG.action(msg % (queue['uuid'], queue['max_bandwidth_rate'],
#                          max_bandwidth_rate))
#
#        if self.action == 'fix':
#            self.nvp.update_queue_maxbw_rate(queue['uuid'],
#                                             max_bandwidth_rate)
#
#        # update queue data structure
#        queue['max_bandwidth_rate'] = max_bandwidth_rate
#        return queue


# class NoVMIDPorts(HunterKillerPortOps):
#    """ adds the vm_id tag to ports which do not have it """
#    def execute(self):
#        self.ports_checked = 0
#        self.start_time = time.time()
#        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
#                     'LogicalPortAttachment', 'LogicalSwitchConfig')
#        nvp_ports = self.nvp.get_ports(relations)
#        instances = self.nova.get_instances()
#        interfaces = self.melange.get_interfaces_hashed_by_id()
#
#        # print what we've managed to get (sanity check)
#        msg = ('Found |%s| ports\n'
#               'Found |%s| intances\n'
#               'Found |%s| interfaces\n'
#               'ctrl-c in 10 seconds if this doesn\'t look right')
#        LOG.output(msg % (len(nvp_ports), len(instances), len(interfaces)))
#        time.sleep(10)
#
#        LOG.output('Walking ports to find missing vmid tags, check '
#                   'loglevel INFO if you want to watch. a . is a port')
#
#        self.walk_port_list(nvp_ports, instances, interfaces)
#        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
#        self.print_calls_made(ports=self.ports_checked)
#
#    def walk_port_list(self, nvp_ports, instances, interfaces):
#        # walk port list checking for ports without vmids
#        # if found, attempt to get vmid and add it
#        # if any error is raised getting instance, ignore the port
#        vmids_fixed = 0
#        for nvp_port in nvp_ports:
#            port = self.create_port_dict(nvp_port)
#            self.ports_checked += 1
#            sys.stdout.write('.')
#            sys.stdout.flush()
#
#            if port['vmid'] is None:
#                LOG.warn('port |%s| has no vm_id tag' % port['uuid'])
#
#                # attempt to get vmid from queue
#                if port['queue'].get('vmid'):
#                    self.port_add_vmid(port, port['queue']['vmid'])
#                    vmids_fixed += 1
#                    continue
#
#                # couldn't get vmid from queue, need instance
#                # ignore port if finding it raises
#                try:
#                    instance = self.get_instance_by_port(port, instances,
#                                                         interfaces)
#                    if instance.get('uuid'):
#                        self.port_add_vmid(port, instance['uuid'])
#                        vmids_fixed += 1
#                except Exception as e:
#                    LOG.error(e)
#
#        LOG.output('vm_ids fixed:', vmids_fixed)
#
#    def port_add_vmid(self, port, vmid):
#        print
#        LOG.action('adding vm_id tag |%s| '
#                   'to port |%s|' % (vmid, port['uuid']))
#        port['tags']['vm_id'] = vmid
#        port['vmid'] = vmid
#        if self.action == 'fix':
#            self.nvp.port_update_tags(port)


class OrphanQueues(HunterKiller):
    """ compares the list of all queues with the list of queues found to be
        associated with ports. deletes queues which are not associated
    """
    def execute(self):
        self.start_time = time.time()

        # get the full list of queues, excepting the qos pools
        all_queues = self.nvp.get_queues()
        self.queues_checked = len(all_queues)

        # get nvp_ports and port_hash manually
        port_relations = ('LogicalQueueConfig', )
        nvp_ports = self.nvp.get_ports(port_relations)
        port_hash = {}
        for port in nvp_ports:
            queue_uuid = port['_relations']['LogicalQueueConfig'].get('uuid')
            if queue_uuid:
                if queue_uuid in port_hash:
                    port_hash[queue_uuid].append(port)
                else:
                    port_hash[queue_uuid] = [port]

        # print what we've managed to get (sanity check)
        msg = ('Found |%s| queues by list\n'
               'Found |%s| ports by list\n'
               'Found |%s| queues associated with ports\n'
               'ctrl-c in 10 seconds if this doesn\'t look right')
        LOG.output(msg % (len(all_queues), len(nvp_ports),
                          len(port_hash.keys())))
        time.sleep(10)
        msg = ('Walking |%s| queues to find orphans, a \'.\' is a queue, '
               'check out loglevel INFO if you want to watch.')
        LOG.output(msg % self.queues_checked)

        # loop through queues, deleting the queues with no associated ports
        orphans = 0
        for queue in all_queues:
            sys.stdout.write('.')
            sys.stdout.flush()
            if not self.get_associated_ports(queue['uuid'], port_hash):
                orphans += 1
                self.delete_queue(queue)

        # print results
        print
        LOG.output('orphans fixed:', orphans)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made(queues=self.queues_checked)

    def get_associated_ports(self, queue_uuid, port_hash=None):
        if port_hash is not None:
            return port_hash[queue_uuid] if queue_uuid in port_hash else []
        port_relations = ('LogicalQueueConfig', )
        return self.nvp.get_ports(port_relations, queue_uuid=queue_uuid)

    def delete_queue(self, queue):
        print
        LOG.action('delete queue |%s|' % queue['uuid'])
        if self.action == 'fix':
            self.nvp.delete_queue(queue['uuid'])


class KillCellQueues(HunterKiller):
    """ uses mysql json to find the instances in a cell
        removes the queues belonging to those instances
    """
    def __init__(self, cell, *args, **kwargs):
        self.cell = cell
        return super(KillCellQueues, self).__init__(*args, **kwargs)

    def execute(self):
        self.start_time = time.time()

        all_queues = self.nvp.get_queues()
        all_instances = self.nova.get_instances()

        # get all this environment's datas
        cells = {}
        for uuid, instance in all_instances.iteritems():
            if instance['cell_name'] in cells.keys():
                cells[instance['cell_name']]['instances'].append(instance)
            else:
                cells[instance['cell_name']] = {'instances': [instance],
                                                'queues': []}
        for queue in all_queues:
            LOG.info('checking queue |%s|' % queue['uuid'])
            vmid = self.nvp.tags_to_dict(queue).get('vmid')
            if not vmid:
                LOG.error('queue |%s| does not have vmid' % queue['uuid'])
            cell = all_instances.get(vmid, {}).get('cell_name')
            cells[cell]['queues'].append(queue)

        # print what we've managed to get (sanity check)
        arrow = '<---'
        msg = ('\nctrl-c in 10 seconds if the following doesn\'t look right\n'
               'Found |%s| queues\n'
               'Found |%s| instances\n'
               'Found these cells (arrows deleting):')
        LOG.output(msg % (len(all_queues), len(all_instances)))
        for k, v in cells.iteritems():
            a = arrow if (k == self.cell or k is None) else ''
            msg = '%25s: %6d instances %6d queues %s'
            LOG.output(msg % (k, len(v['instances']), len(v['queues']), a))
        time.sleep(10)
        msg = ('Walking |%s| queues to remove for cell |%s|, '
               'a \'.\' is a queue, '
               'check out loglevel INFO if you want to watch.')
        LOG.output(msg % (cell, len(all_queues)))

        LOG.output(','.join([c['uuid'] for c in cells[None]['queues']]))
        # print results
#       LOG.output('queues deleted:', deleted_queues)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made(queues=len(all_queues))

    def delete_queue(self, queue):
        print
        LOG.action('delete queue |%s|' % queue['uuid'])
        if self.action == 'fix':
            self.nvp.delete_queue(queue['uuid'])


class DeleteQueueList(HunterKiller):
    """ gets a csv of queue uuids from stdin
        deletes them all
    """
    def __init__(self, file, *args, **kwargs):
        self.queues = file.read().strip().split('\n')
        return super(DeleteQueueList, self).__init__(*args, **kwargs)

    def execute(self):
        self.start_time = time.time()

        LOG.output('Found |%d| queues from input' % len(self.queues))
#        pool = Pool(GEVENT_THREADS)
#        pool.map(self.delete_queue, self.queues)
        for queue in self.queues:
            self.delete_queue(queue)

        LOG.output('queues deleted:', len(self.queues))
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made()

    def delete_queue(self, queue):
        LOG.action('delete queue |%s|' % queue)
        if self.action == 'fix':
            self.nvp.delete_queue(queue)


class RemoveQueueRef(HunterKiller):
    """sets queue uuid to NULL"""
    def execute(self):
        self.start_time = time.time()

        nvp_ports = self.nvp.get_ports(relations=['LogicalPortStatus'])
        LOG.output('Found |%s| ports, a \'.\' is a port' % len(nvp_ports))
        ports = []
        for nvp_port in nvp_ports:
            nvp_status = nvp_port['_relations'].get('LogicalPortStatus')
            port = {'uuid': nvp_port['uuid'],
                    'switch': {'uuid': nvp_status['lswitch']['uuid']}}
            ports.append(port)

        utils.raiselog('need gevent pool to run RemoveQueueRef')
        # pool = Pool(5)
        # pool.map(self.delete_queue_ref, ports)

        print
        LOG.output('ports fixed:', len(ports))
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made()

    def delete_queue_ref(self, port):
        LOG.action('delete queue ref for port |%s|' % port['uuid'])
        if self.action == 'fix':
            sys.stdout.write('.')
            sys.stdout.flush()
            self.nvp.port_delete_queue_ref(port)


class VifIDOnDevice(HunterKiller):
    """ updates vif_id_on_device in the melange db
        with port id from nvp
    """
    def __init__(self, melange_ip, melange_port,
                 melange_ip_user, melange_ip_pass, *args, **kwargs):
        self.mconn = pymysql.connect(host=melange_ip, port=int(melange_port),
                                     user=melange_ip_user,
                                     passwd=melange_ip_pass,
                                     db='melange', autocommit=True)
        return super(VifIDOnDevice, self).__init__(*args, **kwargs)

    def execute(self):
        self.start_time = time.time()

        relations = ('LogicalPortAttachment')
        nvp_ports = self.nvp.get_ports(relations)
        nvp_ports = self.nvp_ports_by_attachment_id(nvp_ports)
        LOG.output('Found |%d| ports in NVP' % len(nvp_ports))

        vifs = self.melange.get_interfaces_with_null_viod()
        LOG.output('Found |%d| interfaces with null viod' % len(vifs))
        vifs = [vif for vif in vifs if self.is_isolated_vif(vif)]
        LOG.output('Found |%d| isolated interfaces with null viod' % len(vifs))

        updated_vifs = 0
        hozed_vifs = 0
        for vif in vifs:
            if vif['id'] in nvp_ports:
                self.update_interface_viod(vif['id'], nvp_ports[vif['id']])
                updated_vifs += 1
            else:
                LOG.action('attachment_id |%s| not found in nvp_ports, '
                           'setting to "hozed"' % vif['id'])
                self.update_interface_viod(vif['id'], 'hozed')
                hozed_vifs += 1
        print
        LOG.output('vifs updated', updated_vifs)
        LOG.output('vifs hozed', hozed_vifs)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made()

    def is_isolated_vif(self, vif):
        try:
            int(vif['tenant_id'])
            return True
        except ValueError:
            return False

    def nvp_ports_by_attachment_id(self, nvp_ports):
        r = {}
        for p in nvp_ports:
            try:
                vif_uuid = p['_relations']['LogicalPortAttachment']['vif_uuid']
                r[vif_uuid] = p['uuid']
            except KeyError:
                pass
        return r

    def update_interface_viod(self, vif_uuid, port_uuid):
        LOG.action('set vif |%s| vif_id_on_device to |%s|' % (vif_uuid,
                                                              port_uuid))
        if self.action == 'fix':
            cur = self.mconn.cursor()

            sql = ('update interfaces set vif_id_on_device="%s" '
                   'where id="%s"')
            cur.execute(sql % (port_uuid, vif_uuid))
            cur.execute(sql)
            for row in cur:
                print row

            cur.close()


class SGPorts(HunterKiller):
    """ sums up ports with security groups """
    def __init__(self, *args, **kwargs):
        super(SGPorts, self).__init__(*args, **kwargs)
        self.summary.start_timer()

    def process(self):
        self.ports_checked = 0
        neutron_ports = self.neutron.get_ports()
        assoc = self.neutron.get_sg_assoc()
        sg = self.neutron.get_sg()
        public_sg_ports = {}
        snet_sg_ports = {}
        isolated_sg_ports = {}

        for uuid, port in neutron_ports.iteritems():
            if uuid in assoc:
                if port['network_id'] == \
                        '11111111-1111-1111-1111-111111111111':
                    snet_sg_ports[uuid] = port
                elif port['network_id'] == \
                        '00000000-0000-0000-0000-000000000000':
                    public_sg_ports[uuid] = port
                else:
                    isolated_sg_ports[uuid] = port

        isolated_sg_port_tenants = set(row['project_id']
                                       for row in isolated_sg_ports.values())

        # print what we've managed to get (sanity check)
        msg = ('Found\n'
               '|%s| public net sg ports\n'
               '|%s| snet sg ports\n'
               '|%s| isolated sg ports\n'
               '|%s| isolated sg port tenants\n'
               '|%s| security groups\n'
               '|%s| sg<->port table len\n'
               '|%s| total ports\n')
        LOG.output(msg % (len(public_sg_ports), len(snet_sg_ports),
                          len(isolated_sg_ports), isolated_sg_port_tenants,
                          len(sg), len(assoc), len(neutron_ports)))
        self.print_calls_made(ports=self.ports_checked)


class IPInfo(HunterKiller):
    """ ip info by tenant instance """
    def __init__(self, *args, **kwargs):
        super(IPInfo, self).__init__(*args, **kwargs)
        self.summary.port_types = True
        self.summary.start_timer()

        self.data = {}

        self.ports = self.neutron.get_ports()
        self.assocs = self.neutron.get_ip_assoc()
        self.ips = self.neutron.get_ip_addresses()
        self.subnets = self.neutron.get_subnets()
        self.instances = self.nova.get_instances()

    def display_summary(self):
        pass

    def add_tenant(self, tenant_id):
        if tenant_id not in self.data:
            self.data[tenant_id] = {}

    def add_tenant_instance(self, tenant_id, instance_id):
        self.add_tenant(tenant_id)
        if instance_id not in self.data[tenant_id]:
            self.data[tenant_id][instance_id] = {'public': [],
                                                 'snet': [],
                                                 'isolated': []}

    def get_port_ips(self, port_id):
        if port_id not in self.assocs:
            return []
        port_ip_ids = [a['ip_address_id'] for a in self.assocs[port_id]]
        port_ips = []
        port_cidrs = []
        for ip_id in port_ip_ids:
            ip = self.ips[ip_id]
            subnet = self.subnets[ip['subnet_id']]
            port_ips.append(ip['address_readable'])
            port_cidrs.append(subnet['_cidr'])

        return zip(port_ips, port_cidrs)

    def summarize_data(self):
        public_only = []
        iso_only = []
        snet_only = []
        pub_iso = []
        pub_snet = []
        snet_iso = []
        triple_threat = []
        none = []
        total = 0

        tenants_easy_iso = []
        tenants_iso_fit16 = []
        tenants_iso_nofit16 = []

        for tenant, instances in self.data.iteritems():
            isolated_ips = []
            public_ips = []
            snet_ips = []

            for instance_uuid, ip_info in instances.iteritems():
                total += 1
                isolated_ips.extend([x for x, y in ip_info['isolated']
                                     if ':' not in x])
                public_ips.extend([x for x, y in ip_info['public']])
                snet_ips.extend([x for x, y in ip_info['snet']])

                if (ip_info['public'] and not ip_info['isolated'] and
                   not ip_info['snet']):
                    public_only.append(instance_uuid)
                elif (ip_info['snet'] and not ip_info['isolated'] and
                      not ip_info['public']):
                    snet_only.append(instance_uuid)
                elif (ip_info['isolated'] and not ip_info['snet'] and
                      not ip_info['public']):
                    iso_only.append(instance_uuid)
                elif (ip_info['public'] and ip_info['isolated'] and
                      not ip_info['snet']):
                    pub_iso.append(instance_uuid)
                elif (not ip_info['public'] and ip_info['isolated'] and
                      ip_info['snet']):
                    snet_iso.append(instance_uuid)
                elif (ip_info['public'] and not ip_info['isolated'] and
                      ip_info['snet']):
                    pub_snet.append(instance_uuid)
                elif (ip_info['public'] and ip_info['isolated'] and
                      ip_info['snet']):
                    triple_threat.append(instance_uuid)
                elif (not ip_info['isolated'] and not ip_info['snet'] and
                      not ip_info['public']):
                    none.append(instance_uuid)
            if len(isolated_ips) < 2:
                tenants_easy_iso.append(tenant)
            elif netaddr.spanning_cidr(isolated_ips).prefixlen >= 16:
                tenants_iso_fit16.append(tenant)
            else:
                tenants_iso_nofit16.append(tenant)
        print 'total instances: %d' % total
        print 'isolated only instances: %d' % len(iso_only)
        print 'public only instances: %d' % len(public_only)
        print 'snet only instances: %d' % len(snet_only)

        print 'public and snet no iso instances: %d' % len(pub_snet)
        print 'public and iso no snet instances: %d' % len(pub_iso)
        print 'snet and iso no public instances: %d' % len(snet_iso)

        print 'pub snet and iso instances: %d' % len(triple_threat)
        print 'no port instances: %d' % len(none)

        print '\ntotal tenants: %d' % len(self.data)
        print 'tenants with 1 or less iso IPs: %d' % len(tenants_easy_iso)
        print 'tenants whose iso fit in /16: %d' % len(tenants_iso_fit16)
        print 'tenants whose iso no fit in /16: %d' % len(tenants_iso_nofit16)

    def process(self):
        self.generate_dataset()

        pprint(self.data)
        self.print_calls_made(ports=self.ports_checked)
        self.summarize_data()
        with open('/home/trey/jsonified_iad', 'w') as f:
            f.write(json.dumps(self.data))

    def generate_dataset(self):
        self.ports_checked = 0
        # pprint(self.neutron.show_tables())
        # print self.neutron.describe_table('quark_subnets')
        # print self.neutron.describe_table('quark_ip_addresses')
        # return

        # print what we've managed to get (sanity check)
        msg = ('Found\n'
               '|%s| ports\n'
               '|%s| instances\n'
               '|%s| ip addresses\n'
               'ctrl-c in 10 seconds if this doesn\'t look right')
        LOG.output(msg % (len(self.ports), len(self.instances),
                          len(self.ips)))

        for uuid, port in self.ports.iteritems():
            self.ports_checked += 1
            instance_id = port['device_id']
            if instance_id not in self.instances:
                # skip orphan port
                self.summary.ports_orphaned += 1
                continue
            tenant_id = port['project_id']
            self.add_tenant_instance(tenant_id, instance_id)
            port_ips = self.get_port_ips(uuid)

            if port['network_id'] == '11111111-1111-1111-1111-111111111111':
                self.data[tenant_id][instance_id]['snet'].extend(port_ips)
                self.summary.ports_snet += 1
            elif port['network_id'] == '00000000-0000-0000-0000-000000000000':
                self.data[tenant_id][instance_id]['public'].extend(port_ips)
                self.summary.ports_public += 1
            else:
                self.data[tenant_id][instance_id]['isolated'].extend(port_ips)
                self.summary.ports_isolated += 1
