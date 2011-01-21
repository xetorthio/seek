package redis.seek;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;
import redis.clients.util.SafeEncoder;

public class Seek {
    private static ShardedJedisPool pool;

    public static void configure(Config config, List<JedisShardInfo> shards) {
        pool = new ShardedJedisPool(config, shards, Hashing.MURMUR_HASH);
    }

    public static ShardedJedisPool getPool() {
        return pool;
    }

    public Index index(String index) {
        return new Index(index);
    }

    public Search search(String index, String order) {
        return new Search(index, order);
    }

    public Search search(String index, String order, ShardField... shardFields) {
        return new Search(index, order, shardFields);
    }

    @SuppressWarnings("unchecked")
    public Info<String, Info<String, Long>> info(String index,
            ShardField... shardFields) {
        Info<String, Info<String, Long>> info = new Info<String, Info<String, Long>>();
        Nest base = (new Nest("indexes")).cat(index);
        if (shardFields != null) {
            for (ShardField shard : shardFields) {
                base.cat(shard.getField()).cat(shard.getValue());
            }
        }
        final Nest idx = base.fork();
        ShardedJedis jedis = getPool().getResource();
        Jedis shard = jedis.getShard(idx.key());
        String stotal = shard.get(idx.cat("info").cat("total").key());
        try {
            info.setTotal(Integer.parseInt(stotal));
        } catch (NumberFormatException e) {
            info.setTotal(0);
        }
        final Map<String, String> facets = shard.hgetAll(idx.cat("info").key());
        Pipeline p = shard.pipelined();
        for (String facetField : facets.keySet()) {
            p.hgetAll(idx.cat("info").cat(facetField).key());
        }
        List<Object> data = p.execute();
        getPool().returnResource(jedis);
        Iterator<Object> iterator = data.iterator();
        for (String facetField : facets.keySet()) {
            List<Object> next = (List<Object>) iterator.next();
            Iterator<Object> it = next.iterator();
            Info<String, Long> m = new Info<String, Long>();
            m.setTotal(Long.parseLong(facets.get(facetField)));
            while (it.hasNext()) {
                m.put(SafeEncoder.encode((byte[]) it.next()), Long
                        .parseLong(SafeEncoder.encode((byte[]) it.next())));
            }
            info.put(facetField, m);
        }
        return info;
    }

    public void clearInfo(String index, ShardField... shardFields) {
        Nest base = (new Nest("indexes")).cat(index);
        if (shardFields != null) {
            for (ShardField shard : shardFields) {
                base.cat(shard.getField()).cat(shard.getValue());
            }
        }
        final Nest idx = base.fork();
        ShardedJedis jedis = getPool().getResource();
        final Jedis shard = jedis.getShard(idx.key());
        shard.del(idx.cat("info").cat("total").key());
        final Map<String, String> facets = shard.hgetAll(idx.cat("info").key());
        Pipeline p = shard.pipelined();
        for (String facetField : facets.keySet()) {
            p.del(idx.cat("info").cat(facetField).key());
        }
        p.del(idx.cat("info").key());
        p.execute();
        getPool().returnResource(jedis);
    }
}
