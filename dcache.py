#!/usr/bin/env python
#encoding:utf8

import sys
from cshash import CSHash
from cache_interface import CacheInterface, RedisCache
from exception import DcacheException

class Dcache(object):
    
    _nodeRing = None
    _cachePool = {}
    _cacheGen = CacheInterface

    def __init__(self, nodes=[], rep=3, cache=RedisCache):
        ''' init all node to caches'''

        if nodes:
            self._nodeRing = CSHash(nodes=nodes, replicas=rep)
            self._cacheGen = cache
            for key, node in self._nodeRing.sorted_nodes():
                if node not in self._cachePool:
                    self._cachePool[node] = self._cacheGen.alloc(node)
        else:
            raise DcacheException, 'try to init with no nodes\n'


    def get_all_nodes(self, ):
        for key, node in self._nodeRing.sorted_nodes():
            yield key, node
        

    def add_node(self, node):
        ''' add node to caches, preload the nearest 
            forward node data to self'''

        for fnode in self._nodeRing.get_fnode(node): 
            try:
                self.preload(fnode, node)
            except Exception, ex:
                raise DcacheException, '%s' % ex
        self._nodeRing.add_node(node)
        self._cachePool[node] = self._cacheGen.alloc(node)



    def remove_node(self, node):
        ''' remove node from caches, preload own data
            to the nearest forword node'''

        for fnode in self._nodeRing.get_fnode(node): 
            try:
                self.preload(node, fnode)
            except Exception, ex:
                raise DcacheException, '%s' % ex
        self._nodeRing.remove_node(node)
        self._cachePool.pop(node)


    def preload(self, fr, to):
        ''' preload for frozen-start effect, in add node or remove node'''

        frclient = self._cacheGen.alloc(fr)
        toclient = self._cacheGen.alloc(to)

        try:
            for key in frclient.keys():
                val = frclient.get(key)
                toclient.set(key, val)
        except Exception, ex:
            raise DcacheException, '%s' % ex
            

    def set(self, key, data):
        ''' load data to caches by key'''

        k, node = self._nodeRing.get_node(key)
        client = self._cachePool[node]
        return client.set(key, data)
        

    def get(self, key):
        ''' get data from caches by key'''

        k, node = self._nodeRing.get_node(key)
        client = self._cachePool[node]
        return client.get(key)


    def checknodes(self,):
        ''' check if all node 
            return None if all ok, else return bad list'''

        bad_nodes = []
        for node in self._cachePool:
            if not self._cachePool[node].ping():
                bad_nodes.append(node)
        if bad_nodes:
            return bad_nodes
        else:
            return None
            
            
