package redis.seek.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.seek.Entry;
import redis.seek.Seek;

public class IndexBenchmark {
    private static final int SEARCHES = 10000;

    public static void main(String[] args) throws IOException {
        Jedis jedis = new Jedis("localhost");
        jedis.flushAll();
        jedis.quit();
        jedis.disconnect();

        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        shards.add(new JedisShardInfo("localhost"));
        Config config = new Config();
        Seek.configure(config, shards);

        Seek seek = new Seek();

        long start = System.nanoTime();

        for (int n = 0; n < SEARCHES; n++) {
            Entry entry = seek.add(String.valueOf(n), 1287278019d);
            entry.addField("c", "MLA31594");
            entry.addField("s", "84689862");
            entry.addTag("b");
            entry
                    .addText("t",
                            "Apple Ipod Classic 160gb 160 8° Generacion 40.000 Canciones!");
            entry.shardBy("s");
            entry.save();
        }
        long elapsed = System.nanoTime() - start;

        jedis.quit();
        jedis.disconnect();

        System.out.println(((1000 * SEARCHES) / (elapsed / 1000000))
                + " indexaciones por segundo");
    }

}
