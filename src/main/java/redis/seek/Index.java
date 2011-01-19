package redis.seek;

import java.util.List;
import java.util.Map;

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
        Nest ndx = idx.fork();
        idx = ndx.cat(id).fork();
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(idx.key());
        try {
            List<String> indexes = shard.lrange(idx.key(), 0, -1);
            Map<String, String> fields = shard.hgetAll(idx.cat("fields").key());
            List<String> tags = shard.lrange(idx.cat("tags").key(), 0, -1);
            Pipeline p = shard.pipelined();
            p.decr(ndx.cat("info").cat("total").key());
            for (String index : indexes) {
                p.zrem(index, id);
            }
            p.del(idx.key());
            for (String tag : tags) {
                p.hincrBy(ndx.cat("info").key(), "tags", -1);
                p.hincrBy(ndx.cat("info").cat("tags").key(), tag, -1);
            }
            for (String field : fields.keySet()) {
                p.hincrBy(ndx.cat("info").key(), field, -1);
                p.hincrBy(ndx.cat("info").cat(field).key(), fields.get(field),
                        -1);
            }
            p.del(idx.cat("fields").key());
            p.del(idx.cat("tags").key());
            p.execute();
            Seek.getPool().returnResource(jedis);
        } catch (Exception e) {
            Seek.getPool().returnBrokenResource(jedis);
            throw new SeekException(e.getMessage());
        }
    }
}