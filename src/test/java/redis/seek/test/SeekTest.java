package redis.seek.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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
import redis.seek.Info;
import redis.seek.Result;
import redis.seek.Search;
import redis.seek.Seek;
import redis.seek.Text;
import redis.seek.Search.Order;

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
        addEntry(98251174l, 1287278019);
        Result result = search(0, 0, 50);

        assertEquals(1, result.getTotalCount());
        assertEquals("98251174", result.getIds().get(0));
    }

    @Test
    public void searchAndPaginate() {
        addEntry(98251174l, 1287278019);
        addEntry(98251175l, 1287278020);
        addEntry(98251176l, 1287278021);

        Result result = search(0, 0, 0);

        assertEquals(3, result.getTotalCount());
        assertEquals(1, result.getIds().size());
        assertEquals("98251176", result.getIds().get(0));
    }

    @Test
    public void reindex() {
        addEntry(98251174l, 1287278019);
        addEntry(98251174l, 1287278019);
        Result result = search(0, 0, 50);

        assertEquals(1, result.getTotalCount());
        assertEquals("98251174", result.getIds().get(0));
    }

    @Test
    public void dontCacheByDefault() {
        addEntry(98251174l, 1287278019);
        search(0, 0, 50);
        Set<String> keys = jedis.keys("*result*");
        assertEquals(0, keys.size());
    }

    @Test
    public void cache() throws InterruptedException {
        addEntry(98251174l, 1287278019);
        Seek seek = new Seek();
        Search search = seek.search(84689862l);
        search.text("title", "ipod 160");
        Result result = search.run(1, 0, 50, Order.DESC);
        Set<String> keys = jedis.keys("*:" + Seek.QUERIES_RESULT);
        assertEquals(1, result.getTotalCount());
        assertEquals(1, keys.size());
        Thread.sleep(2000);
        keys = jedis.keys("*:" + Seek.QUERIES_RESULT);
        assertEquals(0, keys.size());
    }

    @Test
    public void remove() {
        addEntry(98251174l, 1287278019);
        Seek seek = new Seek();
        seek.remove(98251174l, 84689862l);
        Result result = search(0, 0, 1);
        assertEquals(0, result.getTotalCount());
    }

    @Test
    public void stopwords() {
        Set<String> stopwords = new HashSet<String>();
        stopwords.add("un");
        stopwords.add("para");
        Text text = new Text("Un collar para un perro", stopwords);
        Set<String> words = text.getWords();
        assertEquals(2, words.size());
    }

    @Test
    public void searchWithTags() {
        Seek seek = new Seek();
        Entry e = seek.add(123l, new Double(System.currentTimeMillis()));
        e.addShard(2l);
        e.addField("seller_id", "2");
        e.addField("status", "active");
        e.addField("type", "normal");
        e.addText("title", "titulin");
        e.addTag("tagged");
        e.save();

        Search search = seek.search(2l);
        search.field("status", "active");
        search.tag("tagged");
        Result run = search.run();

        assertEquals(1, run.getTotalCount());
    }

    @Test
    public void info() {
        addEntry(98251174l, 1287278019);
        addEntry(98251175l, 1287278020);
        addEntry(98251176l, 1287278021);
        Seek seek = new Seek();
        Info<String, Info<String, Long>> info = seek.info(84689862l);

        assertEquals(3, info.total());
        assertNotNull(info.get("category_id"));
        assertEquals(1, info.get("category_id").size());
        assertEquals(3, info.get("category_id").total());
        assertEquals(3, info.get("category_id").get("MLA31594").longValue());
        assertEquals(3, info.get(Seek.TAGS).get("buy_it_now").longValue());

        seek.remove(98251176l, 84689862l);

        info = seek.info(84689862l);

        assertEquals(2, info.total());
        assertNotNull(info.get("category_id"));
        assertEquals(1, info.get("category_id").size());
        assertEquals(2, info.get("category_id").total());
        assertEquals(2, info.get("category_id").get("MLA31594").longValue());
        assertEquals(2, info.get(Seek.TAGS).get("buy_it_now").longValue());
    }

    @Test
    public void clearInfo() {
        addEntry(98251174l, 1287278019);
        addEntry(98251175l, 1287278020);
        addEntry(98251176l, 1287278021);
        Seek seek = new Seek();
        Info<String, Info<String, Long>> info = seek.info(84689862l);

        assertEquals(3, info.total());
        assertNotNull(info.get("category_id"));
        assertEquals(1, info.get("category_id").size());
        assertEquals(3, info.get("category_id").total());
        assertEquals(3, info.get("category_id").get("MLA31594").longValue());
        assertEquals(3, info.get(Seek.TAGS).get("buy_it_now").longValue());

        seek.clearInfo(84689862l);

        info = seek.info(84689862l);

        assertEquals(0, info.total());
        assertNull(info.get("category_id"));
        assertEquals(0, info.size());
    }

    private Result search(int cache, int start, int end) {
        Seek seek = new Seek();
        Search search = seek.search(84689862l);
        search.field("category_id", "MLA31594", "MLA39056");
        search.tag("buy_it_now", "promotion");
        search.text("title", "ipod 160");
        return search.run(cache, start, end, Search.Order.DESC);
    }

    private Seek addEntry(Long id, double timestamp) {
        Seek seek = new Seek();
        Entry entry = seek.add(id, timestamp);
        entry.addField("category_id", "MLA31594");
        entry.addField("seller_id", "84689862");
        entry.addTag("buy_it_now");
        entry.addText("title",
                "Apple Ipod Classic 160gb 160 8° Generacion 40.000 Canciones!");
        entry.addShard(84689862l);
        entry.save();
        return seek;
    }
}