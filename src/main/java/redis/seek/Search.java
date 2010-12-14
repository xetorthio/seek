package redis.seek;

import java.util.ArrayList;
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

    public Search(String index, String order) {
        this(index, order, null);
    }

    public Search(String index, String order, Shard[] shardFields) {
        this.index = (new Nest("indexes")).cat(index);
        if (shardFields != null) {
            for (Shard shard : shardFields) {
                this.index.cat(shard.getField()).cat(shard.getValue());
                writeQuery(shard.getField(), shard.getValue());
            }
        }
        this.index = this.index.fork();
        shardKey = this.index.key();
        this.index.cat("order").cat(order);
        this.index = this.index.fork();
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
        formulas.add(new DisjunctiveFormula(tags));
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

    @SuppressWarnings("unchecked")
    public List<String> run() {
        String query = getQuery();
        String tmpkey = index.cat("queries").cat(
                String.valueOf(this.hashCode())).cat("tmp").key();
        String rkey = index.cat("queries").cat(query).cat("result").key();
        ZParams zparams = new ZParams();
        zparams.aggregate(Aggregate.MAX);
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(shardKey);
        try {
            long start = System.nanoTime();
            Pipeline pipeline = shard.pipelined();

            List<ConjunctiveFormula> dnf = DNF.convert(formulas);
            for (ConjunctiveFormula conjunctiveFormula : dnf) {
                String[] keys = conjunctiveFormula.getLiterals().toArray(
                        new String[conjunctiveFormula.getLiterals().size()]);
                pipeline.zinterstore(tmpkey, zparams, keys);
                pipeline.zunionstore(rkey, zparams, tmpkey, rkey);
            }
            pipeline.zrange(rkey, 0, 50);
            pipeline.del(rkey, tmpkey);
            List<Object> execute = pipeline.execute();
            List<String> result = prepareResult((List<byte[]>) execute
                    .get(execute.size() - 2));
            long elapsed = System.nanoTime() - start;
            Seek.getPool().returnResource(jedis);
            if (logger.isLoggable(Level.INFO)) {
                logger.info(((double) elapsed / 1000000000d) + " seconds"
                        + " - Query: " + query);
            }
            return result;
        } catch (Exception e) {
            Seek.getPool().returnBrokenResource(jedis);
            throw new SeekException(e.getMessage());
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