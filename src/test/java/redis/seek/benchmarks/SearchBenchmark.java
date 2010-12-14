package redis.seek.benchmarks;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.JedisPool;
import redis.seek.Nest;
import redis.seek.Search;
import redis.seek.Seek;
import redis.seek.Shard;

public class SearchBenchmark {
    private static final String HOST = "localhost";
    private static final int SEARCHES = 100000;

    public static void main(String[] args) throws IOException,
            InterruptedException {

        Config config = new Config();
        JedisPool pool = new JedisPool(config, HOST, 6379, 20000);
        Nest.configure(pool);

        ExecutorService executor = Executors.newFixedThreadPool(50);

        long start = System.nanoTime();
        for (int n = 0; n < SEARCHES; n++) {
            executor.execute(new Runnable() {
                public void run() {
                    Seek seek = new Seek();
                    Search search = seek.search("items", "start_date",
                            new Shard("seller_id", "87660525"));
                    search.text("title", "ipod");
                    search.run();
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

        long elapsed = System.nanoTime() - start;

        System.out.println(((1000 * SEARCHES) / (elapsed / 1000000))
                + " busquedas por segundo");
    }
}