package redis.seek.benchmarks;

import java.io.IOException;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.seek.Entry;
import redis.seek.Index;
import redis.seek.Nest;
import redis.seek.Seek;

public class IndexBenchmark {
    private static final int SEARCHES = 10000;

    public static void main(String[] args) throws IOException {
        Jedis jedis = new Jedis("localhost");
        jedis.flushAll();
        jedis.quit();
        jedis.disconnect();

        Config config = new Config();
        JedisPool pool = new JedisPool(config, "localhost");
        Nest.configure(pool);

        Seek seek = new Seek();
        Index index = seek.index("items");

        long start = System.nanoTime();

        for (int n = 0; n < SEARCHES; n++) {
            Entry entry = index.add(String.valueOf(n));
            entry.addField("category_id", "MLA31594");
            entry.addField("seller_id", "84689862");
            entry.addTag("buy_it_now");
            entry
                    .addText("title",
                            "Apple Ipod Classic 160gb 160 8° Generacion 40.000 Canciones!");
            entry.addOrder("start_date", 1287278019);
            entry.shardBy("seller_id");
            entry.save();
        }
        long elapsed = System.nanoTime() - start;

        jedis.quit();
        jedis.disconnect();

        System.out.println(((1000 * SEARCHES) / (elapsed / 1000000))
                + " indexaciones por segundo");
    }

}
