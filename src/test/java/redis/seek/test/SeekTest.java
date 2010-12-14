package redis.seek.test;

import java.util.List;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.seek.Entry;
import redis.seek.Index;
import redis.seek.Nest;
import redis.seek.Search;
import redis.seek.Seek;
import redis.seek.Shard;

public class SeekTest extends Assert {
    private Jedis jedis;

    @Before
    public void setUp() throws Exception {
        jedis = new Jedis("localhost");
        jedis.flushAll();
        jedis.quit();
        jedis.disconnect();
        Config config = new Config();
        JedisPool pool = new JedisPool(config, "localhost");
        Nest.configure(pool);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void indexAndSearch() {
        addEntry();
        List<String> ids = search();

        assertEquals(1, ids.size());
        assertEquals("MLA98251174", ids.get(0));
    }

    private List<String> search() {
        Seek seek = new Seek();
        Search search = seek.search("items", "start_date", new Shard(
                "seller_id", "84689862"));
        search.field("category_id", "MLA31594", "MLA39056");
        search.tag("buy_it_now", "promotion");
        search.text("title", "ipod 160");
        List<String> ids = search.run();
        return ids;
    }

    private Seek addEntry() {
        Seek seek = new Seek();
        Index index = seek.index("items");
        Entry entry = index.add("MLA98251174");
        entry.addField("category_id", "MLA31594");
        entry.addField("seller_id", "84689862");
        entry.addTag("buy_it_now");
        entry.addText("title",
                "Apple Ipod Classic 160gb 160 8° Generacion 40.000 Canciones!");
        entry.addOrder("start_date", 1287278019);
        entry.shardBy("seller_id");
        entry.save();
        return seek;
    }
}