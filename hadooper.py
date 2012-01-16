#!/usr/bin/python

import argparse
import ConfigParser
import glob
import os
import paramiko
import random
import sys
import time
from novaclient.v1_1 import client

rate_limit_sleep_time = 10
floating_ips_max_check = 10


def create_conf_files(servers):
    
    master_name = ''
    for x in servers.keys():
        if servers[x]['type'] == 'master':
            master_name = servers[x]['name']
        else:
            outputfile = open('Transfer/local_setup_%s.sh' % servers[x]['name'], 'w')
            outputfile.write("""#!/bin/bash

sudo mv /usr/bin/ssh /usr/bin/ssh-orig
sudo su -c "echo '#
ssh-orig -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \"\$@\"
' > /usr/bin/ssh"
sudo chmod 777 /usr/bin/ssh

ip=%s

cd ~
scp -i Transfer/ssh_key -r Transfer $ip:
ssh -i Transfer/ssh_key $ip 'Transfer/setup_slave.sh'

sudo mv /usr/bin/ssh-orig /usr/bin/ssh
exit $?
""" % servers[x]['ip'])
            outputfile.close()

    hdfs = open('Transfer/hdfs-site.xml','w')
    hdfs.write("""<?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

        <configuration>
            <property>
                <name>dfs.replication</name>
                <value>%d</value>
            </property>
        </configuration>
        """ % len(servers))
    hdfs.close()

    mapred = open('Transfer/mapred-site.xml','w')
    mapred.write("""<?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

        <configuration>
            <property>
                <name>mapred.job.tracker</name>
                <value>%s:54311</value>
            </property>
        </configuration>
        """ % master_name)
    mapred.close()    

    core = open('Transfer/core-site.xml','w')
    core.write("""<?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

        <configuration>
            <property>
                <name>hadoop.tmp.dir</name>
                <value>/app/hadoop/tmp</value>
            </property>

            <property>
                <name>fs.default.name</name>
                <value>hdfs://%s:54310</value>
            </property>
        </configuration>""" % master_name)
    core.close()


def connect_to_server(ssh_connect_info, port=22, get_ftp=True):

    server_ip = ssh_connect_info['ip']
    login_name = ssh_connect_info['login']
    key_location = ssh_connect_info['key']
    connection_good = False
    while not connection_good:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(server_ip, username=login_name, key_filename=key_location)

            if get_ftp:
                transport = paramiko.Transport((server_ip, port))
                transport.connect(username = login_name, pkey = paramiko.RSAKey.from_private_key_file(key_location))
                sftp = paramiko.SFTPClient.from_transport(transport)
                return ssh, sftp
            else:
                return ssh                
        except Exception as ex:
            print "Error in ssh connection (will retry in 2 sec): %s" % str(ex)
            time.sleep(2)


def setup_hadoop(ssh, sftp, servers, ssh_connect_info):

    print "Transferring files to cluster"
    sftp = ssh.open_sftp()
    sftp.mkdir('Transfer')
    for file_name in glob.glob('Transfer/*'):
        sftp.put(file_name, file_name)
        sftp.chmod(file_name, 0700)
    time.sleep(10)

    print "Downloading external files on master server"
    stdin, stdout, stderr = ssh.exec_command('~/Transfer/get_files.sh')
    channel = stdout.channel
    status = channel.recv_exit_status()
    ssh.close()
    channel.close()

    print "Setting up servers, this could take a little bit"
    
    for k in servers.keys():
        if servers[k]['type'] == 'slave':
            print "Setting up %s" % servers[k]['name']
            ssh = connect_to_server(ssh_connect_info, get_ftp=False)
            stdin, stdout, stderr = ssh.exec_command('~/Transfer/local_setup_%s.sh' % servers[k]['name'])
            channel = stdout.channel
            status = channel.recv_exit_status()
            ssh.close()
            channel.close()

    print "Setting up master"
    ssh = connect_to_server(ssh_connect_info, get_ftp=False)
    stdin, stdout, stderr = ssh.exec_command('~/Transfer/setup_master.sh')
    channel = stdout.channel
    status = channel.recv_exit_status()
    ssh.close()
    channel.close()

    print "Servers have the boot."


def check_rate_limited(message):
    redo = False
    if str(message) == "This request was rate-limited. (HTTP 413)":
        time.sleep(rate_limit_sleep_time)
        redo = True
    return redo


def create_sec_groups(create_new):

    master_ports = [22, 9000, 9001, 50010, 50020, 50030, 50060, 50070, 50075, 50090]
    slave_ports = [22, 9000, 9001, 50010, 50020, 50030, 50060, 50070, 50075, 50090]
    cidr = '0.0.0.0/0'

    hadoop_master = 'hadoop_master'
    hadoop_slave = 'hadoop_slave'

    if create_new.lower() in ['0', 'f', 'false', 'no', 'off']:
        create_new = False

    if not create_new:
        master_sec_group = ''
        slave_sec_group = ''

        for sec_group in nc.security_groups.list():
            if sec_group.name == hadoop_master:
                master_sec_group = sec_group
            if sec_group.name == hadoop_slave:
                slave_sec_group = sec_group
        if master_sec_group == '' or slave_sec_group == '':
            print "Security groups not present, need to create"
            create_new = True

    if create_new:
        print "Creating security groups"

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
                if check_rate_limited(ex):
                    nc.security_group_rules.create(master_sec_group.id, 'tcp', port, port, '%s' % cidr)
                else:
                    assert None, str(ex)                

        for port in slave_ports:
            try:
                nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)
            except Exception as ex:
                if check_rate_limited(ex):
                    nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)
                else:
                    assert None, str(ex)

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
                    if check_rate_limited(ex):
                        nc.security_group_rules.create(master_sec_group.id, 'tcp', port, port, '%s' % cidr)
                    else:
                        assert None, str(ex)

        for port in slave_ports:
            if slave_after_ports.count(port) == 0:
                #redo port
                try:
                    nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)
                except Exception as ex:
                    if check_rate_limited(ex):
                        nc.security_group_rules.create(slave_sec_group.id, 'tcp', port, port, '%s' % cidr)
                    else:
                        assert None, str(ex)


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


def assign_floating_ip(server_name):
    server = False
    for s in nc.servers.list():
		if s.name == server_name:
			server = s

    if server:
        ip = get_floating_ip()
        if ip:
            server.add_floating_ip(ip)
            return ip

    return False

def get_floating_ip():

    dont_use = ['50.56.12.240', '50.56.12.241', '50.56.12.242', '50.56.12.243']
    
    for floating_ip in nc.floating_ips.list():
        if floating_ip.instance_id == None and str(floating_ip.ip) not in dont_use:
            return floating_ip.ip

    try:
        quota_ips = int(nc.quotas.get(args['tenant']).floating_ips)
        if (len(nc.floating_ips.list()) < quota_ips):
            for x in range(floating_ips_max_check):
                floating_ip = nc.floating_ips.create()
                if floating_ip.ip not in dont_use:
                    return floating_ip.ip

    except Exception as ex:
        if str(ex) == "No more floating ips available. (HTTP 400)":
            return False
        elif str(ex) == "Access was denied to this resource. (HTTP 403)":
            try:
                for x in range(floating_ips_max_check):
                    floating_ip = nc.floating_ips.create()
                    if floating_ip.ip not in dont_use:
                        return floating_ip.ip
            except:
                return False
        else:
            if check_rate_limited(ex):
                try:
                    for x in range(floating_ips_max_check):
                        floating_ip = nc.floating_ips.create()
                        if floating_ip.ip not in dont_use:
                            return floating_ip.ip
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
        time.sleep(2)
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
        return False, exception

    return False


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
            return True, server.name,server.networks['private'][0]
        else:
            return False, '', ''

    except Exception as ex:
        
        if check_rate_limited(ex):
            server = nc.servers.create(
                        image=image,
                        flavor=flavor,
                        name=server_name,
                        key_name=key_name,
                        security_groups=sec_group
                        )
        else:
            assert None, str(ex)
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
    parser.add_argument('-i', '--image', help='default = natty-server-cloudimg-amd64', default='natty-server-cloudimg-amd64')
    parser.add_argument('-l', '--login', help='user to login to image, default = ubuntu', default='ubuntu')
    parser.add_argument('-c', '--cluster', help='cluster size as int, default 4', type=int, default=4)
    parser.add_argument('-sg', '--security', help='Redo hadooper_master and hadooper_slave security groups, default True', default=True)
    args = vars(parser.parse_args())

    if not args['password']:
        Config = ConfigParser.ConfigParser()
        Config.read('config.ini')
        novaconfig = ConfigSectionMap(args['user'])
        args['password'] = novaconfig['nova_password']
        args['tenant'] = novaconfig['nova_project_id']
        args['server'] = novaconfig['nova_url']

    nc = return_nova_object(args)
    master_sec_group_id, slave_sec_group_id = create_sec_groups(args['security'])
    nova_key = get_or_create_key(args['key'])
    
    image = get_image(args['image'])
    assert image, "Image not found"
    
    flavor = get_flavor(args['flavor'])
    assert flavor, "Flavor not found"

    is_master = True
    servers = {}
    for x in range(args['cluster']):
        worked, server_name, server_ip = boot_instance(image, flavor, x, is_master, nova_key.uuid, is_master and [master_sec_group_id] or [slave_sec_group_id])
        ext_ip = ''
        if is_master:
            assign_floating_ip_return = assign_floating_ip(server_name)
            if assign_floating_ip_return:
                ext_ip = assign_floating_ip_return

        if worked:
            servers[x] = {'name': server_name, 'type': is_master and 'master' or 'slave', 'ip': server_ip, 'ext_ip': ext_ip}
        is_master = False

    if not os.path.exists('Transfer'):
        os.mkdir('Transfer')
    transfer_files_to_keep = ['setup_master.sh', 'setup_slave.sh', 'bashrc_add', 'hadoop-env.sh', 'get_files.sh']
    for file in glob.glob('Transfer/*'):
        if file.split('/')[-1] not in transfer_files_to_keep:
            os.system('rm %s' % file)
    server_config_file = open('Transfer/servers.conf','w')
    os.system('cp %s Transfer/ssh_key' % args['key'])
    os.system('cp %s.pub Transfer/ssh_key.pub' % args['key'])
    ssh = ''

    host_file_additions = open('Transfer/add_to_hosts','w')
    masters_file = open('Transfer/masters','w')
    slaves_file = open('Transfer/slaves','w')

    master_server = ''
    for x in servers.keys():

        slaves_file.write('%s\n' % servers[x]['name'])
        host_file_additions.write('%s   %s\n' % (servers[x]['ip'], servers[x]['name']))
        server_config_file.write('[%s]\n' % servers[x]['name'])

        for k in servers[x].keys():
            if k != 'name':
                server_config_file.write('%s= %s\n' % (k, servers[x][k]))
        server_config_file.write('login= %s\n\n' % (args['login']))

        if servers[x]['type'] == 'master':
            masters_file.write('%s\n' % servers[x]['name'])
            masters_file.close()
            server_ip =  servers[x]['ext_ip']
            login_name = args['login']
            key_location = args['key']

    slaves_file.close()
    create_conf_files(servers)
    ssh_connect_info = {'ip': server_ip, 'login': login_name, 'key': key_location}
    ssh, sftp = connect_to_server(ssh_connect_info ) 

    host_file_additions.close()
    server_config_file.close()
    setup_hadoop(ssh, sftp, servers, ssh_connect_info)