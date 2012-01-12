#!/usr/bin/env python

import argparse
import ConfigParser
import random
import sys
import time
from novaclient.v1_1 import client


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


def actions(action, expected_state, server, time_limit=60):
    action_done = False
    action_start = time.time()

    try:
        if action == "pause":
            server.pause()
        elif action == "unpause":
            server.unpause()
        elif action == "suspend":
            server.suspend()
        elif action == "resume":
            server.resume()

        while not action_done and time.time() - action_start < time_limit:
            if return_server_status(server) == expected_state:
                action_done = True
            else:
                time.sleep(3)

        action_msg = ""
        if not action_done:
            action_msg = "Server not %s within %d sec" % (action, time_limit)
        return action_done, action_msg

    except Exception as except_msg:
        return False, except_msg




def shutdownInstances():
    destroy_time = 60
    try:
        for server in nc.servers.list():
            status = get_server_status(server) 
            if status in ['PAUSED','SUSPENDED']:
                actions(status == 'PAUSED' and 'unpause' or 'resume', 'ACTIVE', server)
            print "Deleting %s"%server.id
            server_id = server.id
            server.delete()

            start = time.time()
            is_deleted = False
            while not is_deleted and time.time() - start < destroy_time:
                if not any([s.id == server_id for s in nc.servers.list()]):
                    is_deleted = True
                time.sleep(2)
            print "Deleted"
        return True
    except Exception as ex:
        print ex
        return False



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hadooper, an automated hadoop cluster installer for openstack')
    parser.add_argument('user', metavar='user', help='-u USERNAME where username is defined a config file, config.ini, if additional connection args not provided')
    parser.add_argument('-p', '--password',  default=None)
    parser.add_argument('-t', '--tenant', help='defaults to username as defined by -u', default=None)
    parser.add_argument('-s', '--server', default=None)
    args = vars(parser.parse_args())

    if not args['password']:
        Config = ConfigParser.ConfigParser()
        Config.read('config.ini')
        novaconfig = ConfigSectionMap(args['user'])
        args['password'] = novaconfig['nova_password']
        args['tenant'] = novaconfig['nova_project_id']
        args['server'] = novaconfig['nova_url']

    nc = return_nova_object(args)
    shutdownInstances()
