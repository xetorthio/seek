package redis.seek.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import redis.seek.Search.Order;
import redis.seek.Seek;
import redis.seek.Text;
import redis.seek.search.ConjunctiveFormula;
import redis.seek.search.DNF;
import redis.seek.search.DisjunctiveFormula;

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
        config.maxActive = 100;
        config.maxIdle = 100;
        config.minIdle = 100;
        config.testOnBorrow = true;
        Seek.configure(config, shards);
    }

    @After
    public void tearDown() {
        Seek.getPool().destroy();
    }

    @Test
    public void indexAndSearch() {
        addEntry("MLA98251174", 1287278019);
        Result result = search(0, 0, 50);

        assertEquals(1, result.getTotalCount());
        assertEquals("MLA98251174", result.getIds().get(0));
    }

    @Test
    public void searchAndPaginate() {
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251175", 1287278020);
        addEntry("MLA98251176", 1287278021);

        Result result = search(0, 0, 0);

        assertEquals(3, result.getTotalCount());
        assertEquals(1, result.getIds().size());
        assertEquals("MLA98251176", result.getIds().get(0));
    }

    @Test
    public void reindex() {
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251174", 1287278019);
        Result result = search(0, 0, 50);

        assertEquals(1, result.getTotalCount());
        assertEquals("MLA98251174", result.getIds().get(0));
    }

    @Test
    public void dnf() {
        List<DisjunctiveFormula> formulas = new ArrayList<DisjunctiveFormula>();
        formulas.add(new DisjunctiveFormula("a"));
        formulas.add(new DisjunctiveFormula("b"));
        formulas.add(new DisjunctiveFormula("c"));
        List<ConjunctiveFormula> convert = DNF.convert(formulas);

        assertEquals(1, convert.size());
        assertEquals(3, convert.get(0).getLiterals().size());
        assertEquals("a", convert.get(0).getLiterals().get(0));
        assertEquals("b", convert.get(0).getLiterals().get(1));
        assertEquals("c", convert.get(0).getLiterals().get(2));

        formulas = new ArrayList<DisjunctiveFormula>();
        formulas.add(new DisjunctiveFormula("a", "b"));
        formulas.add(new DisjunctiveFormula("c", "d"));
        convert = DNF.convert(formulas);

        assertEquals(4, convert.size());
        assertEquals(2, convert.get(0).getLiterals().size());
        assertEquals("a", convert.get(0).getLiterals().get(0));
        assertEquals("c", convert.get(0).getLiterals().get(1));
        assertEquals(2, convert.get(1).getLiterals().size());
        assertEquals("a", convert.get(1).getLiterals().get(0));
        assertEquals("d", convert.get(1).getLiterals().get(1));
        assertEquals(2, convert.get(2).getLiterals().size());
        assertEquals("b", convert.get(2).getLiterals().get(0));
        assertEquals("c", convert.get(2).getLiterals().get(1));
        assertEquals(2, convert.get(3).getLiterals().size());
        assertEquals("b", convert.get(3).getLiterals().get(0));
        assertEquals("d", convert.get(3).getLiterals().get(1));

        formulas = new ArrayList<DisjunctiveFormula>();
        formulas.add(new DisjunctiveFormula("a"));
        formulas.add(new DisjunctiveFormula("b"));
        formulas.add(new DisjunctiveFormula("c", "d"));
        convert = DNF.convert(formulas);

        assertEquals(2, convert.size());
        assertEquals(3, convert.get(0).getLiterals().size());
        assertEquals("a", convert.get(0).getLiterals().get(0));
        assertEquals("b", convert.get(0).getLiterals().get(1));
        assertEquals("c", convert.get(0).getLiterals().get(2));
        assertEquals(3, convert.get(1).getLiterals().size());
        assertEquals("a", convert.get(1).getLiterals().get(0));
        assertEquals("b", convert.get(1).getLiterals().get(1));
        assertEquals("d", convert.get(1).getLiterals().get(2));
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
        Search search = seek.search("84689862");
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
        addEntry("MLA98251174", 1287278019);
        Seek seek = new Seek();
        seek.remove("MLA98251174", "84689862");
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
        Entry e = seek.add("123", new Double(System.currentTimeMillis()));
        e.shardBy("seller_id");
        e.addField("seller_id", "2");
        e.addField("status", "active");
        e.addField("type", "normal");
        e.addText("title", "titulin");
        e.addTag("tagged");
        e.save();

        Search search = seek.search("2");
        search.field("status", "active");
        search.tag("tagged");
        Result run = search.run();

        assertEquals(1, run.getTotalCount());
    }

    @Test
    public void info() {
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251175", 1287278020);
        addEntry("MLA98251176", 1287278021);
        Seek seek = new Seek();
        Info<String, Info<String, Long>> info = seek.info("84689862");

        assertEquals(3, info.total());
        assertNotNull(info.get("category_id"));
        assertEquals(1, info.get("category_id").size());
        assertEquals(3, info.get("category_id").total());
        assertEquals(3, info.get("category_id").get("MLA31594").longValue());
        assertEquals(3, info.get("tags").get("buy_it_now").longValue());

        seek.remove("MLA98251176", "84689862");

        info = seek.info("84689862");

        assertEquals(2, info.total());
        assertNotNull(info.get("category_id"));
        assertEquals(1, info.get("category_id").size());
        assertEquals(2, info.get("category_id").total());
        assertEquals(2, info.get("category_id").get("MLA31594").longValue());
        assertEquals(2, info.get("tags").get("buy_it_now").longValue());
    }

    @Test
    public void clearInfo() {
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251175", 1287278020);
        addEntry("MLA98251176", 1287278021);
        Seek seek = new Seek();
        Info<String, Info<String, Long>> info = seek.info("84689862");

        assertEquals(3, info.total());
        assertNotNull(info.get("category_id"));
        assertEquals(1, info.get("category_id").size());
        assertEquals(3, info.get("category_id").total());
        assertEquals(3, info.get("category_id").get("MLA31594").longValue());
        assertEquals(3, info.get("tags").get("buy_it_now").longValue());

        seek.clearInfo("84689862");

        info = seek.info("84689862");

        assertEquals(0, info.total());
        assertNull(info.get("category_id"));
        assertEquals(0, info.size());
    }

    @Test
    public void parallelQueries() throws InterruptedException {
        final AtomicInteger errors = new AtomicInteger();
        addEntry("MLA98251174", 1287278019);
        addEntry("MLA98251175", 1287278020);
        addEntry("MLA98251176", 1287278021);

        ExecutorService pool = Executors.newFixedThreadPool(50);
        for (int n = 0; n < 100; n++) {
            pool.execute(new Runnable() {
                public void run() {
                    Result result = search(0, 0, 100);
                    if (3 != result.getTotalCount()) {
                        errors.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);
        assertEquals(0, errors.get());
    }

    private Result search(int cache, int start, int end) {
        Seek seek = new Seek();
        Search search = seek.search("84689862");
        search.field("category_id", "MLA31594", "MLA39056");
        search.tag("buy_it_now", "promotion");
        search.text("title", "ipod 160");
        return search.run(cache, start, end, Search.Order.DESC);
    }

    private Seek addEntry(String id, double timestamp) {
        Seek seek = new Seek();
        Entry entry = seek.add(id, timestamp);
        entry.addField("category_id", "MLA31594");
        entry.addField("seller_id", "84689862");
        entry.addTag("buy_it_now");
        entry.addText("title",
                "Apple Ipod Classic 160gb 160 8° Generacion 40.000 Canciones!");
        entry.shardBy("seller_id");
        entry.save();
        return seek;
    }
}