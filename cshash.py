#!/usr/bin/env python
#encoding:utf8

import sys
from hashlib import md5
from exception import CSHashException

class CSHash(object):
        
    _replicas = 3
    _hashring = {}
    _sorted_keys =[] 
    
    def __init__(self, nodes=None, replicas=3):
        '''manage hash ring
        '''
        self._replicas = replicas
        
        if nodes:
            for node in nodes:
                self.add_node(node)
      
    def add_node(self, node):
        ''' add node to consistent hash ring'''

        for rep in xrange(0, self._replicas):
            key = self.gen_key('%s:%s' % (node, rep))
            self._sorted_keys.append(key)
            self._hashring[key] = node
        self._sorted_keys.sort()
 

    def remove_node(self, node):
        ''' remove node from consistent hash ring'''

        if not self._hashring:
            raise CSHashException, 'hashring is empty\n'

        for rep in xrange(0, self._replicas):
            key = self.gen_key('%s:%s' % (node, rep))
            self._hashring.pop(key)
            self._sorted_keys.remove(key)


    def get_node(self, str_key):
        ''' get string-type node from hash ring by input key'''

        if not self._hashring:
            raise CSHashException, 'hashring is empty\n'
        
        key = self.gen_key(str_key)

        for hashval in self._sorted_keys:
            if key <= hashval:
                return hashval, self._hashring[hashval]

        return self._sorted_keys[0], \
                self._hashring[self._sorted_keys[0]]

    def get_fvnode(self, node):
        ''' get the input node forward virtual node in hash ring'''

        if not self._hashring:
            raise CSHashException, 'hashring is empty\n'
        
        for rep in xrange(0, self._replicas):
            node_key = self.gen_key('%s:%s' % (node, rep))
        
            for hashval in self._sorted_keys:
                if node_key <= hashval:
                    yield hashval, self._hashring[hashval]
                    break
            if node_key > self._sorted_keys[-1:]:
                yield self._sorted_keys[0], \
                        self._hashring[self._sorted_keys[0]]


    def get_fnode(self, node):
        ''' get the forward real node in hash ring'''
        nodes = set()
        for key, vnode in self.get_fvnode(node):
            nodes.add(vnode)    
        for node in nodes:
            yield node


    def gen_key(self, str_key):
        ''' generate the md5 key by input key'''

        return long(md5(str_key).hexdigest(), 16)
        

    def get_nodes(self, str_key):
        ''' get the input node all forward nodes from hash ring'''

        if not self._hashring:
            raise CSHashException, 'hashring is empty\n'

        key = self.gen_key(str_key)

        for idx, hashval in self._sorted_keys:
            if key <= hashval:
                for i in self._sorted_keys[idx:]:
                    yield (self._sorted_keys[i], \
                        self._hashring[self._sorted_keys[i]])
                break

    def size(self,):
        return len(self._hashring)

    def sorted_nodes(self,):
        for key in self._sorted_keys:
            yield key, self._hashring[key]


