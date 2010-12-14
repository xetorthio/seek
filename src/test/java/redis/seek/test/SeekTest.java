package redis.seek.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.seek.Entry;
import redis.seek.Index;
import redis.seek.Search;
import redis.seek.Seek;
import redis.seek.ShardField;

public class SeekTest extends Assert {
    private Jedis jedis;

    @Before
    public void setUp() throws IOException {
        jedis = new Jedis("localhost");
        jedis.flushAll();
        jedis.quit();
        jedis.disconnect();
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        shards.add(new JedisShardInfo("localhost"));
        Config config = new Config();
        Seek.configure(config, shards);
    }

    @After
    public void tearDown() {
        Seek.getPool().destroy();
    }

    @Test
    public void indexAndSearch() {
        addEntry("MLA98251174", 1287278019);
        List<String> ids = search(0, 0, 50);

        assertEquals(1, ids.size());
        assertEquals("MLA98251174", ids.get(0));
    }

    @Test
    public void searchAndPaginate() {
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251175", 1287278020);
        addEntry("MLA98251176", 1287278021);

        List<String> ids = search(0, 0, 0);

        assertEquals(1, ids.size());
        assertEquals("MLA98251174", ids.get(0));
    }

    @Test
    public void reindex() {
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251174", 1287278019);
        List<String> ids = search(0, 0, 50);

        assertEquals(1, ids.size());
        assertEquals("MLA98251174", ids.get(0));
    }

    @Test
    public void dontCacheByDefault() {
        addEntry("MLA98251174", 1287278019);
        search(0, 0, 50);
        Set<String> keys = jedis.keys("*result*");
        assertEquals(0, keys.size());
    }

    @Test
    public void cache() throws InterruptedException {
        addEntry("MLA98251174", 1287278019);
        Seek seek = new Seek();
        Search search = seek.search("items", "start_date", new ShardField(
                "seller_id", "84689862"));
        search.text("title", "ipod 160");
        List<String> result = search.run(1, 0, 50);
        Set<String> keys = jedis.keys("*result*");
        assertEquals(1, result.size());
        assertEquals(1, keys.size());
        Thread.sleep(2000);
        keys = jedis.keys("*result*");
        assertEquals(0, keys.size());
    }

    @Test
    public void remove() {
        addEntry("MLA98251174", 1287278019);
        Seek seek = new Seek();
        seek.index("items").remove("MLA98251174",
                new ShardField("seller_id", "84689862"));
        List<String> result = search(0, 0, 1);
        assertEquals(0, result.size());
    }

    private List<String> search(int cache, int start, int end) {
        Seek seek = new Seek();
        Search search = seek.search("items", "start_date", new ShardField(
                "seller_id", "84689862"));
        search.field("category_id", "MLA31594", "MLA39056");
        search.tag("buy_it_now", "promotion");
        search.text("title", "ipod 160");
        List<String> ids = search.run(cache, start, end);
        return ids;
    }

    private Seek addEntry(String id, double timestamp) {
        Seek seek = new Seek();
        Index index = seek.index("items");
        Entry entry = index.add(id);
        entry.addField("category_id", "MLA31594");
        entry.addField("seller_id", "84689862");
        entry.addTag("buy_it_now");
        entry.addText("title",
                "Apple Ipod Classic 160gb 160 8° Generacion 40.000 Canciones!");
        entry.addOrder("start_date", timestamp);
        entry.shardBy("seller_id");
        entry.save();
        return seek;
    }
}