package redis.seek;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;

public class Entry {
    private Index index;
    private String id;
    private Map<String, String> fields = new HashMap<String, String>();
    private Map<String, Set<String>> textFields = new HashMap<String, Set<String>>();
    private Set<String> tags = new HashSet<String>();
    private Map<String, Double> orders = new HashMap<String, Double>();
    private String[] shardFields;

    public Entry(Index index, String id) {
        this.index = index;
        this.id = id;
    }

    public void addField(String field, String value) {
        fields.put(field, value);
    }

    public void addTag(String tag) {
        tags.add(tag);
    }

    public void addText(String field, String text) {
        textFields.put(field, (new Text(text)).getWords());
    }

    public void addText(String field, String text, Set<String> stopwords) {
        textFields.put(field, (new Text(text, stopwords)).getWords());
    }

    public void save() {
        Nest idx = (new Nest("indexes")).cat(index.getName());
        ShardField[] sfields = new ShardField[shardFields.length];
        int s = 0;
        for (String field : shardFields) {
            idx.cat(field).cat(fields.get(field));
            sfields[s++] = new ShardField(field, fields.get(field));
        }
        idx = idx.fork();
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(idx.key());
        try {
            if (shard.exists(idx.cat(id).key())) {
                index.remove(id, sfields);
            }
            Pipeline p = shard.pipelined();
            for (Map.Entry<String, Double> order : orders.entrySet()) {
                Nest i = idx.cat("order").cat(order.getKey()).fork();
                for (Map.Entry<String, String> field : fields.entrySet()) {
                    i.cat(field.getKey()).cat(field.getValue());
                    String key = i.key();
                    p.zadd(key, order.getValue(), id);
                    p.rpush(idx.cat(id).key(), key);
                    p.hset(idx.cat(id).cat("fields").key(), field.getKey(),
                            field.getValue());

                    // adds on facets
                    p.hincrBy(idx.cat("info").cat(field.getKey()).key(), field
                            .getValue(), 1);
                    p.hincrBy(idx.cat("info").key(), field.getKey(), 1);
                }
                for (String tag : tags) {
                    i.cat(tag);
                    String key = i.key();
                    p.zadd(key, order.getValue(), id);
                    p.rpush(idx.cat(id).key(), key);
                    p.rpush(idx.cat(id).cat("tags").key(), tag);

                    // adds on facets
                    p.hincrBy(idx.cat("info").cat("tags").key(), tag, 1);
                    p.hincrBy(idx.cat("info").key(), "tags", 1);
                }
                for (Map.Entry<String, Set<String>> field : textFields
                        .entrySet()) {
                    for (String word : field.getValue()) {
                        i.cat(field.getKey()).cat(word);
                        String key = i.key();
                        p.zadd(key, order.getValue(), id);
                        p.rpush(idx.cat(id).key(), key);
                    }
                }
            }
            p.incr(idx.cat("info").cat("total").key());
            p.execute();
            Seek.getPool().returnResource(jedis);
        } catch (Exception e) {
            Seek.getPool().returnBrokenResource(jedis);
            throw new SeekException(e.getMessage());
        }
    }

    public void addOrder(String name, double value) {
        orders.put(name, value);
    }

    public void shardBy(String... fields) {
        shardFields = fields;
    }
}