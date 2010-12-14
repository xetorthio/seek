package redis.seek;

import java.util.List;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;

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

    public Search search(String index, String order, Shard... shardFields) {
        return new Search(index, order, shardFields);
    }
}
