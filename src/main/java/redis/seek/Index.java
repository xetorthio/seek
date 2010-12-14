package redis.seek;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;

public class Index {
    private String name;

    public String getName() {
        return name;
    }

    public Index(String name) {
        this.name = name;
    }

    public Entry add(String id) {
        return new Entry(this, id);
    }

    public void remove(String id, ShardField... shardFields) {
        Nest idx = (new Nest("indexes")).cat(name);
        for (ShardField field : shardFields) {
            idx.cat(field.getField()).cat(field.getValue());
        }
        idx = idx.cat(id).fork();
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(idx.key());
        try {
            List<String> indexes = shard.lrange(idx.key(), 0, -1);
            Pipeline p = shard.pipelined();
            for (String index : indexes) {
                p.zrem(index, id);
            }
            p.del(idx.key());
            p.execute();
            Seek.getPool().returnResource(jedis);
        } catch (Exception e) {
            Seek.getPool().returnBrokenResource(jedis);
            throw new SeekException(e.getMessage());
        }
    }
}