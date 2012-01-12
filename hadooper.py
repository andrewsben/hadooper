#!/usr/bin/env python

import argparse
import ConfigParser
import os
import paramiko
import random
import sys
import time
from novaclient.v1_1 import client

rate_limit_sleep_time = 10

def setup_hadoop(server_dict):
    print "do stuff"

def create_sec_groups():

    print "Creating security groups"

    master_ports = [22, 9000, 9001, 50010, 50020, 50030, 50060, 50070, 50075, 50090]
    slave_ports = [22, 9000, 9001, 50010, 50020, 50030, 50060, 50070, 50075, 50090]
    cidr = '0.0.0.0/0'

    hadoop_master = 'hadoop_master'
    hadoop_slave = 'hadoop_slave'

    for sec_group in nc.security_groups.list():
        if sec_group.name in [hadoop_master, hadoop_slave]:
            sec_group_name = sec_group.name
            sec_group.delete()
            is_deleted = False
            while not is_deleted:
                if not any([sec_group_name == sg.name for sg in nc.security_groups.list()]):
                    is_deleted = True
                else:
                    time.sleep(2)

    is_created = False
    master_sec_group = nc.security_groups.create(hadoop_master, hadoop_master)
    while not is_created:
        if any([hadoop_master == sg.name for sg in nc.security_groups.list()]):
            is_created = True
        else:
            time.sleep(2)

    is_created = False
    slave_sec_group = nc.security_groups.create(hadoop_slave, hadoop_slave)
    while not is_created:
        if any([hadoop_slave == sg.name for sg in nc.security_groups.list()]):
            is_created = True
        else:
            time.sleep(2)

    #create master rules, watch for rate limit crap
    for port in master_ports:
        try:
            nc.security_group_rules.create(master_sec_group.id, 'tcp', port, port, '%s' % cidr)
        except Exception as ex:
            if str(ex) == "This request was rate-limited. (HTTP 413)":
                time.sleep(rate_limit_sleep_time)
                nc.security_group_rules.create(master_sec_group.id, 'tcp', port, port, '%s' % cidr)

    for port in slave_ports:
        try:
            nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)
        except Exception as ex:
            if str(ex) == "This request was rate-limited. (HTTP 413)":
                time.sleep(rate_limit_sleep_time)
                nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)

    #probably should run back through all the rules for master_sec_group and slave_sec_group and verify that the rules are in 
    #can do a call on master_sec_group.get() probably
    time.sleep(rate_limit_sleep_time)

    for sg in nc.security_groups.list():
        if sg.name in [hadoop_master, hadoop_slave]:
            if sg.name == hadoop_master:
                master_sec_group = sg
            else:
                slave_sec_group = sg

    master_after_ports = []
    master_sec_group.get()
    for rule in master_sec_group.rules:
        master_after_ports.append(int(rule['from_port']))

    slave_after_ports = []
    slave_sec_group.get()
    for rule in slave_sec_group.rules:
        slave_after_ports.append(int(rule['from_port']))

    for port in master_ports:
        if master_after_ports.count(port) == 0:
            #redo port
            try:
                nc.security_group_rules.create(master_sec_group.id, 'tcp', port, port, '%s' % cidr)
            except Exception as ex:
                if str(ex) == "This request was rate-limited. (HTTP 413)":
                    time.sleep(rate_limit_sleep_time)
                    nc.security_group_rules.create(master_sec_group.id, 'tcp', port, port, '%s' % cidr)

    for port in slave_ports:
        if slave_after_ports.count(port) == 0:
            #redo port
            try:
                nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)
            except Exception as ex:
                if str(ex) == "This request was rate-limited. (HTTP 413)":
                    time.sleep(rate_limit_sleep_time)
                    nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)

    return master_sec_group.name, slave_sec_group.name


def return_nova_object(args_dict):
    try:
        nc = client.Client(args_dict['user'], args_dict['password'], args_dict['tenant'], args_dict['server'])
        return nc
    except:
        assert None, "Unable to create nova client"


def ConfigSectionMap(section):
    dict1 = {}
    options = ''
    try:
        options = Config.options(section)
    except Exception as ex:
        assert None, "Could not get information from config.ini"
    finally:
        for option in options:
            try:
                dict1[option] = Config.get(section, option)
                if dict1[option] == -1:
                    DebugPrint("skip: %s" % option)
            except Exception as ex:
                print "hey hey hey"
                print("exception on %s!" % option)
                dict1[option] = None
    return dict1


def get_server_status(server):
    try:
        server.get()
        status = server.status
    except Exception as except_msg:
        if except_msg == 'This request was rate-limited. (HTTP 413)':
            print "Rate limited: Waiting 5 seconds"
            time.sleep(5)
            return get_server_status(server)
    else:
        return status


def get_image(image_name):
    for i in nc.images.list():
        if i.name == image_name:
            return i
    return False        

def get_flavor(flavor_name):
    for f in nc.flavors.list():
        if f.name == flavor_name:
            return f
    return False        


def get_floating_ip():

    for floating_ip in nc.floating_ips.list():
        if floating_ip.instance_id == None:
            return floating_ip

    try:
        quota_ips = int(nc.quotas.get(args['tenant']).floating_ips)
        if (len(nc.floating_ips.list()) < quota_ips):
            return nc.floating_ips.create()

    except Exception as except_msg:
        error_msg = str(except_msg)
        if error_msg == "No more floating ips available. (HTTP 400)":
            return False
        elif error_msg == "Access was denied to this resource. (HTTP 403)":
            try:
                floating_ip = nc.floating_ips.create()
                return floating_ip
            except:
                return False

    return False


def get_or_create_key(key_name):

    print "Setting up keys"
    del_nova_key = False
    public_key = ''

    if not os.path.exists(key_name) or not os.path.exists('%s.pub' % key_name):
        del_nova_key = True
        dummy = os.popen('ssh-keygen -t rsa -P "" -f %s' % key_name).readlines()
        public_key = open('%s.pub' % key_name, 'r').readlines()[0]
    else:
        public_key = open('%s.pub' % key_name, 'r').readlines()[0]

    nova_key = False
    for k in nc.keypairs.list():
        if k.uuid == key_name.split('/')[-1]:
            nova_key = k
    if del_nova_key and nova_key:
        nova_key.delete()
        time.sleep()
        key_deleted = False
        while not key_deleted:
            go_on = False
            for k in nc.keypairs.list():
                if k.uuid == key_name.split('/')[-1]:
                    go_on = True
            key_deleted = go_on

    created_key = nova_key
    if not nova_key or del_nova_key:
        created_key = nc.keypairs.create(key_name.split('/')[-1],public_key)
    return created_key



def check_booted(server):
    boot_time = 60
    booted = False
    boot_start = time.time()
    success_msg = 'cloud-init boot finished'

    try:
        while not booted and time.time() - boot_start < boot_time:
            console_output = nc.servers.get_console_output(server.id)
            if success_msg in console_output:
                booted = True
            time.sleep(3)
        print "Boot successful"    
        if not booted:
            print "Boot failed"
        return booted, booted and '' or \
                    "Server not booted within %d sec" % (boot_time)
    except Exception as exception:
        print "Exception in get_console_output"
        print exception
        return false, exception



def boot_instance(image, flavor, server_number, is_master, key_name, sec_group):
    try:
        server_name = "hadooper-%s-%d" % (is_master and "master" or "slave", server_number)

        server = nc.servers.create(
                    image=image,
                    flavor=flavor,
                    name=server_name,
                    key_name=key_name,
                    security_groups=sec_group
                    )
        print "Booting server %s" % server_name

        if server:
            while get_server_status(server) != "ACTIVE":
                time.sleep(3)
            while len(server.networks) == 0:
                time.sleep(3)
                server.get()
            floating_ip = get_floating_ip()
            if floating_ip:
                server.add_floating_ip(floating_ip)
            return True, server.name,floating_ip.ip
        else:
            return False, '', ''

    except Exception as except_msg:
        assert None, str(except_msg)
        except_stuff = """
        quota_exceeded = "InstanceLimitExceeded: Instance quota "
        quota_exceeded += "exceeded. You cannot run any "
        quota_exceeded += "more instances of this type. "
        quota_exceeded += "(HTTP 413)"
        if str(except_msg) == "This request was rate-limited. (HTTP 413)":
            print "rate limited"
            time.sleep(10)
            return boot_random_instance()
        elif str(except_msg) == quota_exceeded:
            print "Quota exceeded, removing instances"
            for server in nc.servers.list():
                destroy_instance(server)
            return boot_random_instance()
        else:
            print str(except_msg)
        return False, except_msg
        """


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hadooper, an automated hadoop cluster installer for openstack')
    parser.add_argument('user', metavar='user', help='-u USERNAME where username is defined a config file, config.ini, if additional connection args not provided')
    parser.add_argument('-p', '--password',  default=None)
    parser.add_argument('-t', '--tenant', help='defaults to username as defined by -u', default=None)
    parser.add_argument('-s', '--server', default=None)
    parser.add_argument('-k', '--key', help='ssh key to use or create, defaults to hadooper_key', default='hadooper_key')
    parser.add_argument('-f', '--flavor', help='default = m1.medium', default='m1.medium')
    parser.add_argument('-i', '--image', help='default = oneiric-server-cloudimg-amd64', default='oneiric-server-cloudimg-amd64')
    parser.add_argument('-c', '--cluster', help='cluster size as int, default 4', type=int, default=4)
    args = vars(parser.parse_args())

    if not args['password']:
        Config = ConfigParser.ConfigParser()
        Config.read('config.ini')
        novaconfig = ConfigSectionMap(args['user'])
        args['password'] = novaconfig['nova_password']
        args['tenant'] = novaconfig['nova_project_id']
        args['server'] = novaconfig['nova_url']

    nc = return_nova_object(args)

    master_sec_group_id, slave_sec_group_id = create_sec_groups()

    nova_key = get_or_create_key(args['key'])

    image = get_image(args['image'])
    assert image, "Image not found"
    
    flavor = get_flavor(args['flavor'])
    assert flavor, "Flavor not found"

    is_master = True
    servers = {}
    for x in range(args['cluster']):
        worked, server_name, server_ip = boot_instance(image, flavor, x, is_master, nova_key.uuid, is_master and [master_sec_group_id] or [slave_sec_group_id])
        if worked:
            servers[x] = {'name': server_name, 'type': is_master and 'master' or 'slave', 'ip': server_ip}
        is_master = False
    print servers
