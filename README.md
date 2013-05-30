dcache
======

distribute cache program using consistent hash, with inheriting cache_interface, you can use every store you want (redis, memcache...) as the real cache storage


dependent on
======

redis-py :
  redis is the default in cache type, so dcache dependent on redis-py, if the real condition is based on other storage, redis-py is not required

example
======

nodes = ['localhost:6379', 'localhost:6378', 'localhost:6377']

dc = Dcache(nodes=nodes, rep=2, cache=RedisCache)

"rep" is virtual node num in use, RedisCache is a cache class inherit cache_interface, every user defined cache inherited from cache_interface can be loaded in dcache
