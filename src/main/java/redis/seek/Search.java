package redis.seek;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.ZParams.Aggregate;
import redis.clients.util.SafeEncoder;
import redis.seek.search.ConjunctiveFormula;
import redis.seek.search.DNF;
import redis.seek.search.DisjunctiveFormula;

public class Search {
    private List<DisjunctiveFormula> formulas = new ArrayList<DisjunctiveFormula>();
    private Nest index;
    private String shardKey;
    private StringBuilder sb;
    private final Logger logger = Logger.getLogger(Search.class.getName());

    public enum Order {
        ASC, DESC
    }

    public Search() {
        this(null);
    }

    public Search(String[] shards) {
        this.index = new Nest("");
        if (shards != null) {
            for (String shard : shards) {
                this.index.cat(shard);
            }
        }
        this.index = this.index.fork();
        shardKey = this.index.key();
    }

    private void writeQuery(String field, Object... values) {
        if (sb == null) {
            sb = new StringBuilder();
        } else {
            sb.append(" AND ");
        }
        sb.append(field);
        sb.append(":[");
        sb.append(join(values, " OR "));
        sb.append("]");
    }

    private static String join(final Object[] objs, final String delimiter) {
        StringBuilder buffer = null;
        for (Object element : objs) {
            if (buffer == null) {
                buffer = new StringBuilder();
            } else {
                buffer.append(delimiter);
            }
            buffer.append(element);
        }
        return buffer.toString();
    }

    public void field(String field, String... values) {
        DisjunctiveFormula formula = new DisjunctiveFormula();
        for (String value : values) {
            formula.addLiteral(index.cat(field).cat(value).key());
        }
        formulas.add(formula);
        writeQuery(field, values);
    }

    public void tag(String... tags) {
        DisjunctiveFormula formula = new DisjunctiveFormula();
        for (String tag : tags) {
            formula.addLiteral(index.cat(tag).key());
        }
        formulas.add(formula);
        writeQuery("tag", tags);
    }

    public void text(String field, String text) {
        DisjunctiveFormula formula = new DisjunctiveFormula();
        Set<String> words = (new Text(text)).getWords();
        for (String word : words) {
            formula.addLiteral(index.cat(field).cat(word).key());
        }
        formulas.add(formula);
        writeQuery(field, words.toArray());
    }

    private String getQuery() {
        return sb.toString();
    }

    public Result run() {
        return run(0, 0, 49, Order.DESC);
    }

    @SuppressWarnings("unchecked")
    public Result run(int cache, int start, int end, Order order) {
        String query = getQuery();
        String tmpkey = index.cat(Seek.QUERIES).cat(query)
                .cat(Seek.QUERIES_TMP).cat(String.valueOf(Thread.currentThread().getId())).key();
        String rkey = index.cat(Seek.QUERIES).cat(query)
                .cat(Seek.QUERIES_RESULT).cat(String.valueOf(Thread.currentThread().getId())).key();
        String rkeyCached = index.cat(Seek.QUERIES).cat(query)
                .cat(Seek.QUERIES_RESULT).key();

        ZParams zparams = new ZParams();
        zparams.aggregate(Aggregate.MAX);
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(shardKey);
        try {
            long tstart = System.nanoTime();
            List<String> result = null;
            long count = 0;
            Set<String> range = jedis.zrange(rkeyCached, start, end);
            if (range != null && range.size() > 0) {
                count = jedis.zcard(rkeyCached);
                result = Arrays.asList(range.toArray(new String[range.size()]));
            } else {
                Pipeline pipeline = shard.pipelined();
                List<ConjunctiveFormula> dnf = DNF.convert(formulas);
                for (ConjunctiveFormula conjunctiveFormula : dnf) {
                    String[] keys = conjunctiveFormula.getLiterals()
                            .toArray(
                                    new String[conjunctiveFormula.getLiterals()
                                            .size()]);
                    pipeline.zinterstore(tmpkey, zparams, keys);
                    pipeline.zunionstore(rkey, zparams, tmpkey, rkey);
                }
                if (order.equals(Order.ASC)) {
                    pipeline.zrange(rkey, start, end);
                } else {
                    pipeline.zrevrange(rkey, start, end);
                }
                List<byte[]> rawresult = null;
                if (cache == 0) {
                    pipeline.del(rkey, tmpkey);
                    List<Object> execute = pipeline.execute();
                    // get zrange or zrevrange result
                    rawresult = (List<byte[]>) execute.get(execute.size() - 2);
                    // get the last zunionstore output, which is the number of
                    // elements in the result
                    count = (Long) execute.get(execute.size() - 3);
                } else {
                    pipeline.del(tmpkey);
                    pipeline.rename(rkey, rkeyCached);
                    pipeline.expire(rkeyCached, cache);
                    pipeline.del(rkey);
                    List<Object> execute = pipeline.execute();
                    // get zrange or zrevrange result
                    rawresult = (List<byte[]>) execute.get(execute.size() - 5);
                    // get the last zunionstore output, which is the number of
                    // elements in the result
                    count = (Long) execute.get(execute.size() - 6);
                }
                result = prepareResult(rawresult);
            }

            double elapsedTime = ((double) (System.nanoTime() - tstart) / (1000000000d));
            Seek.getPool().returnResource(jedis);
            if (logger.isLoggable(Level.INFO)) {
                logger.info(elapsedTime + " seconds" + " - Query: " + query);
            }
            return new Result(count, result, elapsedTime);
        } catch (Exception e) {
            Seek.getPool().returnResource(jedis);
            throw new SeekException(e);
        }
    }

    private List<String> prepareResult(List<byte[]> members) {
        List<String> result = new ArrayList<String>();
        Iterator<byte[]> iterator = members.iterator();
        while (iterator.hasNext()) {
            result.add(SafeEncoder.encode(iterator.next()));
        }
        return result;
    }
}