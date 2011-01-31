package redis.seek;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;

public class Entry {
    private String id;
    private Double order;
    private Map<String, String> fields = new HashMap<String, String>();
    private Map<String, Set<String>> textFields = new HashMap<String, Set<String>>();
    private Set<String> tags = new HashSet<String>();
    private String[] shardFields;
    private Seek seek;

    public Entry(Seek seek, String id, Double order) {
        this.id = id;
        this.seek = seek;
        this.order = order;
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
        Nest idx = new Nest("");
        String[] sfields = new String[shardFields.length];
        int s = 0;
        for (String field : shardFields) {
            idx.cat(fields.get(field));
            sfields[s++] = fields.get(field);
        }
        idx = idx.fork();
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(idx.key());
        try {
            if (shard.exists(idx.cat(id).key())) {
                seek.remove(id, sfields);
            }
            Pipeline p = shard.pipelined();
            for (Map.Entry<String, String> field : fields.entrySet()) {
                idx.cat(field.getKey()).cat(field.getValue());
                String key = idx.key();
                p.zadd(key, order, id);
                p.rpush(idx.cat(id).key(), key);
                p.hset(idx.cat(id).cat(Seek.FIELDS).key(), field.getKey(),
                        field.getValue());

                // adds on facets
                p.hincrBy(idx.cat(Seek.INFO).cat(field.getKey()).key(), field
                        .getValue(), 1);
                p.hincrBy(idx.cat(Seek.INFO).key(), field.getKey(), 1);
            }
            for (String tag : tags) {
                idx.cat(tag);
                String key = idx.key();
                p.zadd(key, order, id);
                p.rpush(idx.cat(id).key(), key);
                p.rpush(idx.cat(id).cat(Seek.TAGS).key(), tag);

                // adds on facets
                p.hincrBy(idx.cat(Seek.INFO).cat(Seek.TAGS).key(), tag, 1);
                p.hincrBy(idx.cat(Seek.INFO).key(), Seek.TAGS, 1);
            }
            for (Map.Entry<String, Set<String>> field : textFields.entrySet()) {
                for (String word : field.getValue()) {
                    idx.cat(field.getKey()).cat(word);
                    String key = idx.key();
                    p.zadd(key, order, id);
                    p.rpush(idx.cat(id).key(), key);
                }
            }
            p.incr(idx.cat(Seek.INFO).cat(Seek.TOTAL).key());
            p.execute();
            Seek.getPool().returnResource(jedis);
        } catch (Exception e) {
            Seek.getPool().returnBrokenResource(jedis);
            throw new SeekException(e);
        }
    }

    public void shardBy(String... fields) {
        shardFields = fields;
    }
}