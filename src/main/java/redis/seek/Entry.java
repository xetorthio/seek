package redis.seek;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
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

    public void save() {
        Nest idx = (new Nest("indexes")).cat(index.getName());
        for (String field : shardFields) {
            idx.cat(field).cat(fields.get(field));
        }
        idx = idx.fork();
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(idx.key());
        try {
            for (Map.Entry<String, Double> order : orders.entrySet()) {
                Nest i = idx.cat("order").cat(order.getKey()).fork();
                for (Map.Entry<String, String> field : fields.entrySet()) {
                    i.cat(field.getKey()).cat(field.getValue());
                    shard.zadd(i.key(), order.getValue(), id);
                }
                for (String tag : tags) {
                    i.cat(tag);
                    shard.zadd(i.key(), order.getValue(), id);
                }
                for (Map.Entry<String, Set<String>> field : textFields
                        .entrySet()) {
                    for (String word : field.getValue()) {
                        i.cat(field.getKey()).cat(word);
                        shard.zadd(i.key(), order.getValue(), id);
                    }
                }
            }
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