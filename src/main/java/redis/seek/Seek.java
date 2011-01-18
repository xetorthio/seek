package redis.seek;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
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
    public Info getInfo(String index, ShardField... shardFields) {
        Info info = new Info();
        Nest base = (new Nest("indexes")).cat(index);
        if (shardFields != null) {
            for (ShardField shard : shardFields) {
                base.cat(shard.getField()).cat(shard.getValue());
            }
        }
        final Nest idx = base.fork();
        ShardedJedis jedis = getPool().getResource();
        String stotal = jedis.get(idx.cat("info").cat("total").key());
        try {
            info.setTotal(Integer.parseInt(stotal));
        } catch (NumberFormatException e) {
            info.setTotal(0);
        }
        final Map<String, String> facets = jedis.hgetAll(idx.cat("info").key());
        List<Object> data = jedis.pipelined(new ShardedJedisPipeline() {
            public void execute() {
                for (String facetField : facets.keySet()) {
                    hgetAll(idx.cat("info").cat(facetField).key());
                }
            }
        });
        getPool().returnResource(jedis);
        Iterator<Object> iterator = data.iterator();
        for (String facetField : facets.keySet()) {
            List<Object> next = (List<Object>) iterator.next();
            Iterator<Object> it = next.iterator();
            Map<String, Long> m = new HashMap<String, Long>();
            while (it.hasNext()) {
                m.put(SafeEncoder.encode((byte[]) it.next()), Long
                        .parseLong(SafeEncoder.encode((byte[]) it.next())));
            }
            info.put(facetField, m);
        }
        return info;
    }
}
