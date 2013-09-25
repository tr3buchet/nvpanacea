import logging
import sys
import time
from datetime import timedelta
from gevent.pool import Pool

import querylib


LOG = logging.getLogger(__name__)
LOG.action = lambda s, *args, **kwargs: LOG.log(33, s, *args, **kwargs)


zone_qos_pool_map = {'public': 'pub_base_rate',
                     'private': 'snet_base_rate'}

GEVENT_THREADS = 10


class HunterKiller(object):
    def __init__(self, action,
                 nvp_url, nvp_username, nvp_password,
                 nova_url, nova_username, nova_password,
                 melange_url, melange_username, melange_password):
        self.action = action
        self.nvp = querylib.NVP(nvp_url, nvp_username, nvp_password)
        self.nova = querylib.Nova(nova_url, nova_username, nova_password)
        self.melange = querylib.Melange(melange_url, melange_username,
                                        melange_password)

    def execute(self, *args, **kwargs):
        raise NotImplementedError()

    def print_calls_made(self, ports=None, queues=None):
        msg = ''
        if ports is not None:
            msg = '%s ports processed\n' % ports
        if queues is not None:
            msg += '%s queues_checked\n' % queues

        msg += ('%s calls to nvp\n%s calls to melange\n%s calls to nova\n'
                'time taken %s')
        print msg % (self.nvp.calls, self.melange.calls, self.nova.calls,
                     self.time_taken)


class HunterKillerPortOps(HunterKiller):
    def get_instance_by_port(self, port, instances, interfaces):
        instance_id = port['vmid']

        # vmid tag wasn't on port, check the queue
        if not instance_id:
            instance_id = port['queue'].get('vmid')

        # get instance_id from melange interfaces if we don't already have it
        if not instance_id:
            interface = interfaces.get(port['vif_uuid']) \
                if port['vif_uuid'] else None
            # if we found an interface, grab it's device_id
            if interface:
                instance_id = interface['device_id']

        # if we ended up with an instance_id, see if we have an instance
        # and return it
        if instance_id:
            return instances.get(instance_id) or {}
        return {}

    def is_isolated_switch(self, switch):
        # if switch  has a qos pool, it is not isolated
        return not 'qos_pool' in switch['tags']

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
        LOG.warn(msg % (switch['uuid'], switch['name']))

        # lswitch didn't have a qos_pool, have to use transport zone
        # read: isolated nw port switch
        zone_id = switch['transport_zone_uuid']
        qos_pool = self.get_qos_pool_from_transport_zone_map(zone_id)
        if qos_pool:
            return qos_pool

        LOG.error('qos pool couldnt be found using transport zone map either!')

    def create_port_dict(self, nvp_port):
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
                'instance': {}}

        return port


class OrphanPorts(HunterKillerPortOps):
    """ deletes orphan ports as they are found """
    def execute(self):
        self.ports_checked = 0
        self.start_time = time.time()
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        nvp_ports = self.nvp.get_ports(relations)
        instances = self.nova.get_instances_hashed_by_id()
        interfaces = self.melange.get_interfaces_hashed_by_id()

        # print what we've managed to get (sanity check)
        msg = ('Found |%s| ports\n'
               'Found |%s| intances\n'
               'Found |%s| interfaces\n'
               'ctrl-c in 10 seconds if this doesn\'t look right')
        print msg % (len(nvp_ports), len(instances), len(interfaces))
        time.sleep(10)
        msg = ('Walking |%s| ports to find orphans, '
               'check out loglevel INFO if you want to watch. a . is a port')
        print msg % len(nvp_ports)

        self.walk_port_list(nvp_ports, instances, interfaces)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made(ports=self.ports_checked)

    def walk_port_list(self, nvp_ports, instances, interfaces):
        # walk port list populating port to get the instance
        # if any error is raised getting instance, ignore the port
        # if port is deemed orphan, fix it
        orphans_fixed = 0
        down_down = {}
        for nvp_port in nvp_ports:
            port = self.create_port_dict(nvp_port)
            self.ports_checked += 1
            sys.stdout.write('.')
            sys.stdout.flush()

            if not (port['link_status_up'] or port['fabric_status_up']):
                try:
                    port['instance'] = self.get_instance_by_port(port,
                                                                 instances,
                                                                 interfaces)
                    if port['instance'].get('vm_state') in down_down:
                        down_down[port['instance'].get('vm_state')] += 1
                    else:
                        down_down[port['instance'].get('vm_state')] = 1
                except Exception as e:
                    print
                    LOG.error(e)
                    continue

                if self.is_orphan_port(port):
                    self.delete_port(port)
                    orphans_fixed += 1

        print '\norphans fixed:', orphans_fixed
        print '\ndown_down instance status counts:', down_down

    def is_orphan_port(self, port):
        # no instance is orphan
        if not port['instance']:
            LOG.warn('found port |%s| w/no instance' % port['uuid'])
            return True

        # deleted instance is orphan
        if port['instance'].get('vm_state') == 'deleted':
            msg = 'found port |%s| w/instance |%s| in state |%s|'
            LOG.warn(msg % (port['uuid'], port['instance'].get('uuid'),
                            port['instance']['vm_state']))
            return True

        # otherwise not an orphan
        return False

    def delete_port(self, port):
        print
        LOG.action('delete port |%s|' % port['uuid'])
        if self.action == 'fix':
            self.nvp.delete_port(port)


class RepairQueues(HunterKillerPortOps):
    """ creates a tree port/queue information and repairs queues.

        can do one of these things:
        1) create a queue and associate port(s) with it
        2) associate a port to an existing queue
        3) update the max_bandwidth_rate on a port's queue
    """
    def execute(self):
        self.ports_checked = 0
        self.start_time = time.time()
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        nvp_ports = self.nvp.get_ports(relations)
        instances = self.nova.get_instances_hashed_by_id(join_flavor=True)
        interfaces = self.melange.get_interfaces_hashed_by_id()

        # print what we've managed to get (sanity check)
        msg = ('Found |%s| ports\n'
               'Found |%s| intances\n'
               'Found |%s| interfaces\n'
               'ctrl-c in 10 seconds if this doesn\'t look right')
        print msg % (len(nvp_ports), len(instances), len(interfaces))
        time.sleep(10)

        tree = self.populate_tree(nvp_ports, instances, interfaces)
        self.fix(tree)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made(ports=self.ports_checked)

    def populate_tree(self, nvp_ports, instances, interfaces):
        tree = {}
        for nvp_port in nvp_ports:
            self.ports_checked += 1
            port = self.create_port_dict(nvp_port)

            # repairing a queue requires instance and flavor, so
            # we need all instances
            # otherwise we could get vmid queue (more efficient)
            # NOTE: all ports w/instance will be in the tree for queue repair
            port['instance'] = self.get_instance_by_port(port, instances,
                                                         interfaces)
            # ignore ports with no instance (the orphans)
            if port['instance']:
                self.add_port_to_tree(port, tree)

        return tree

    def add_port_to_tree(self, port, tree):
        instance_id = port['instance']['uuid']

        if instance_id in tree:
            tree[instance_id].append(port)
        else:
            tree[instance_id] = [port]

    def fix(self, tree):
        queues_repaired = 0
        no_queues_fixed = 0
        for instance_id, ports in tree.iteritems():
            for port in ports:
                if port['queue']:
                    # port has queue, make sure it's squared away
                    if self.ensure_port_queue_bw(port):
                        queues_repaired += 1
                else:
                    # port had no queue, make or associate with one
                    self.repair_port_queue(tree, instance_id, port)
                    no_queues_fixed += 1
        print 'queues repaired:', queues_repaired
        print 'no_queues fixed:', no_queues_fixed

    def repair_port_queue(self, tree, instance_id, port):
        if port['public']:
            msg = 'creating queue for public port |%s|'
            LOG.action(msg % port['uuid'])
            queue = self.create_queue(port)
            self.associate_queue(port, queue)
        elif port['snet'] or port['isolated']:
            # these ports need to share a queue
            # other ports will be snet or isolated nw w/ queue
            other_ports = [p for p in tree[instance_id]
                           if p['queue'] and
                           (p['snet'] or p['isolated'])]
            if other_ports:
                # found port(s) with a queue to share
                msg = ('associating queue for snet/isolated port '
                       '|%s| with snet/isolated port |%s| queue')
                other_port = other_ports[0]
                LOG.action(msg % (port['uuid'], other_port['uuid']))
                self.associate_queue(port, other_port['queue'])
            else:
                # no ports had queue to share, create and associate
                msg = 'creating queue for snet/isolated port |%s|'
                LOG.action(msg % port['uuid'])
                queue = self.create_queue(port)
                self.associate_queue(port, queue)

    def ensure_port_queue_bw(self, port):
        # returns True if action was taken
        if port['queue']['ignored']:
            return False
        # see if queue max_bw_rate is squared away
        queue = port['queue']
        mbwr = queue['max_bandwidth_rate']
        calc_mbwr = self.calculate_max_bandwidth_rate(port)
        if mbwr == calc_mbwr:
            return False
        else:
            # it isn't, go ahead and square that away
            self.update_queue_max_bandwidth_rate(queue, calc_mbwr)
            return True

    def calculate_max_bandwidth_rate(self, port):
        try:
            rxtx_factor = port['instance'].get('rxtx_factor')
            rxtx_base = port['qos_pool'].get('max_bandwidth_rate')
            max_bandwidth_rate = int(rxtx_base) * int(rxtx_factor)
        except (ValueError, TypeError):
            LOG.error('rxtx_cap calculation failed. base: |%s|, factor: |%s|' %
                      (rxtx_base, rxtx_factor))
            return None
        return max_bandwidth_rate

    def create_queue(self, port):
        LOG.action('create queue for port |%s|' % port['uuid'])
        if port['queue']:
            LOG.error('port |%s| already has a queue!' % port['uuid'])
            return

        queue = {'display_name': port['qos_pool']['uuid'],
                 'vmid': port['vmid'] or port['instance']['uuid']}

        max_bandwidth_rate = self.calculate_max_bandwidth_rate(port)
        if max_bandwidth_rate is None:
            return
        queue['max_bandwidth_rate'] = max_bandwidth_rate

        LOG.action('creating queue: |%s|' % queue)
        if self.action == 'fix':
            nvp_queue = self.nvp.create_queue(**queue)
            if nvp_queue:
                return {'uuid': nvp_queue['uuid'],
                        'max_bandwidth_rate': nvp_queue['max_bandwidth_rate'],
                        'vmid': self.nvp.tags_to_dict(nvp_queue).get('vmid')}
            else:
                LOG.error('queue creation failed for port |%s|' % port['uuid'])
        else:
            # return a fake uuid for noop mode
            queue['uuid'] = 'fake'
            return queue

    def associate_queue(self, port, queue):
        LOG.action('associating port |%s| with queue |%s|' %
                   (port['uuid'], queue['uuid']))
        if self.action == 'fix':
            self.nvp.port_update_queue(port, queue['uuid'])
            port['queue'] = queue
        else:
            # in fixnoop, we need to "associate" the queue for similar
            # behavior to what happens in fix mode
            port['queue'] = queue

    def update_queue_max_bandwidth_rate(self, queue, max_bandwidth_rate):
        msg = 'update max_bandwidth_rate for queue |%s| from |%s| to |%s|'
        LOG.action(msg % (queue['uuid'], queue['max_bandwidth_rate'],
                          max_bandwidth_rate))

        if self.action == 'fix':
            self.nvp.update_queue_maxbw_rate(queue['uuid'], max_bandwidth_rate)

        # update queue data structure
        queue['max_bandwidth_rate'] = max_bandwidth_rate
        return queue


class NoVMIDPorts(HunterKillerPortOps):
    """ adds the vm_id tag to ports which do not have it """
    def execute(self):
        self.ports_checked = 0
        self.start_time = time.time()
        relations = ('LogicalPortStatus', 'LogicalQueueConfig',
                     'LogicalPortAttachment', 'LogicalSwitchConfig')
        nvp_ports = self.nvp.get_ports(relations)
        instances = self.nova.get_instances_hashed_by_id()
        interfaces = self.melange.get_interfaces_hashed_by_id()

        # print what we've managed to get (sanity check)
        msg = ('Found |%s| ports\n'
               'Found |%s| intances\n'
               'Found |%s| interfaces\n'
               'ctrl-c in 10 seconds if this doesn\'t look right')
        print msg % (len(nvp_ports), len(instances), len(interfaces))
        time.sleep(10)

        print ('Walking ports to find missing vmid tags, '
               'check out loglevel INFO if you want to watch. a . is a port')

        self.walk_port_list(nvp_ports, instances, interfaces)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made(ports=self.ports_checked)

    def walk_port_list(self, nvp_ports, instances, interfaces):
        # walk port list checking for ports without vmids
        # if found, attempt to get vmid and add it
        # if any error is raised getting instance, ignore the port
        vmids_fixed = 0
        for nvp_port in nvp_ports:
            port = self.create_port_dict(nvp_port)
            self.ports_checked += 1
            sys.stdout.write('.')
            sys.stdout.flush()

            if port['vmid'] is None:
                LOG.warn('port |%s| has no vm_id tag' % port['uuid'])

                # attempt to get vmid from queue
                if port['queue'].get('vmid'):
                    self.port_add_vmid(port, port['queue']['vmid'])
                    vmids_fixed += 1
                    continue

                # couldn't get vmid from queue, need instance
                # ignore port if finding it raises
                try:
                    instance = self.get_instance_by_port(port, instances,
                                                         interfaces)
                    if instance.get('uuid'):
                        self.port_add_vmid(port, instance['uuid'])
                        vmids_fixed += 1
                except Exception as e:
                    LOG.error(e)

        print '\nvm_ids fixed:', vmids_fixed

    def port_add_vmid(self, port, vmid):
        print
        LOG.action('adding vm_id tag |%s| to port |%s|' % (vmid, port['uuid']))
        port['tags']['vm_id'] = vmid
        port['vmid'] = vmid
        if self.action == 'fix':
            self.nvp.port_update_tags(port)


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
        print msg % (len(all_queues), len(nvp_ports), len(port_hash.keys()))
        time.sleep(10)
        msg = ('Walking |%s| queues to find orphans, a \'.\' is a queue, '
               'check out loglevel INFO if you want to watch.')
        print msg % self.queues_checked

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
        print 'orphans fixed:', orphans
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
        all_instances = self.nova.get_instances_hashed_by_id()

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
        print msg % (len(all_queues), len(all_instances))
        for k, v in cells.iteritems():
            a = arrow if (k == self.cell or k is None) else ''
            msg = '%25s: %6d instances %6d queues %s'
            print msg % (k, len(v['instances']), len(v['queues']), a)
        time.sleep(10)
        msg = ('Walking |%s| queues to remove for cell |%s|, '
               'a \'.\' is a queue, '
               'check out loglevel INFO if you want to watch.')
        print msg % (cell, len(all_queues))

        print ','.join([c['uuid'] for c in cells[None]['queues']])
        # print results
        print
#        print 'queues deleted:', deleted_queues
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

        print 'Found |%d| queues from input' % len(self.queues)
        pool = Pool(GEVENT_THREADS)
        pool.map(self.delete_queue, self.queues)
#        for queue in self.queues:
#            self.delete_queue(queue)

        print
        print 'queues deleted:', len(self.queues)
        self.time_taken = timedelta(seconds=(time.time() - self.start_time))
        self.print_calls_made()

    def delete_queue(self, queue):
        LOG.action('delete queue |%s|' % queue)
        if self.action == 'fix':
            self.nvp.delete_queue(queue)
