#!/usr/bin/env python
#encoding:utf8

from redis import StrictRedis, BlockingConnectionPool

class CacheInterface(object):
    
    def __init__(self, conn_pool):
        ''' for single client, init host and port;
            for client pool, init pool'''
        pass

    

    @classmethod
    def alloc(self, node, max_connections, timeout, db):
        ''' @node: "host:port" string
            @db: specific data store in cache
            return self Object'''
        pass
        
    
    def get_instance(self):
        ''' for single client, return Redis;
            for client pool, return item in pool'''
        pass
    
    def keys(self,):
        ''' yield all keys in cache'''
        pass

    def set(self, key, val):
        ''' set val in cache by key'''
        pass

    def get(self, key):
        ''' get val in cache by key'''
        pass

    @classmethod
    def ping(self, node):
        pass

    def __del__(self,):
        ''' release sth'''
        pass



class RedisCache(CacheInterface):
    
    _c = None
    _pool = None
    
    
    def __init__(self, conn_pool):
        ''' for single client, init host and port;
            for client pool, init pool'''
        self._c = StrictRedis(connection_pool=conn_pool)
        self._pool = pool

    
    @classmethod
    def alloc(self, node, max_connections=2**16, timeout=20, db=0):
        host = node.split(':')[0]
        port = int(node.split(':')[1])
        pool = BlockingConnectionPool(max_connections=max_connections, 
            timeout=timeout, **{'host': host, 'port': port, 'db': db})
        
        return RedisCache(pool)

    def get_instance(self,):
        ''' for single client, return Redis;
            for client pool, return item in pool'''
        return self._c


    def keys(self,):
        ''' yield all keys in cache'''
        for key in self._c.keys():
            yield key


    def set(self, key, val):
        ''' set val in cache by key'''
        self._c.set(key, val)


    def get(self, key):
        ''' get val in cache by key'''
        return self._c.get(key)


    @classmethod
    def ping(self, node):
        return self._c.ping()


    def __del__(self,):
        ''' release connections'''
        self._pool.disconnect()
        del self._pool

         


if __name__ == '__main__':
    redis = RedisCache.alloc('localhost:6379').get_instance()
    for key in redis.keys():
        print '%s\t%s' % (key, redis.get(key))
