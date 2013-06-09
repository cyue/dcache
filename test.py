#!/usr/bin/env python
#encoding:utf8

import sys
from dcache import Dcache
from cache_interface import CacheInterface, RedisCache
from exception import DcacheException, CSHashException


def test_get(client, key):
    '''node is virtual node'''
        try: 
            client.get(key)
        except DcacheException, ex:
            bads = client.checknodes()
            if bads:
                for node in bads:
                    client.remove_node(node)
            else:
                sys.stderr.write('%s\n' % ex)


        for key, node in client.get_all_nodes():
            print key, node

def test_set(client, key, data):
    '''set data'''
    try:
        client.set(key, data)
    except DcacheException, ex:
        bads = client.checknodes()
        if bads:
            for node in bads:
                client.remove_node(node)
        else:
            sys.stderr.write('%s\n' % ex)


    for key, node in client.get_all_nodes():
        print key, node



if __name__ == '__main__':
    hosts = ['localhost:9001', 'localhost:9002', 'localhost:9003'] 
    client = Dcache(nodes=hosts, rep=3, cache=RedisCache)
    client.test_set('111', 'aaa')
    client.test_get('111')
