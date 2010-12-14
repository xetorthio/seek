package redis.seek;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    public Search(String index, String order) {
        this(index, order, null);
    }

    public Search(String index, String order, Shard[] shardFields) {
        this.index = (new Nest("indexes")).cat(index);
        if (shardFields != null) {
            for (Shard shard : shardFields) {
                this.index.cat(shard.getField()).cat(shard.getValue());
            }
        }
        this.index = this.index.fork();
        shardKey = this.index.key();
        this.index.cat("order").cat(order);
        this.index = this.index.fork();
    }

    public void field(String field, String... values) {
        DisjunctiveFormula formula = new DisjunctiveFormula();
        for (String value : values) {
            formula.addLiteral(index.cat(field).cat(value).key());
        }
        formulas.add(formula);
    }

    public void tag(String... tags) {
        formulas.add(new DisjunctiveFormula(tags));
    }

    public void text(String field, String text) {
        DisjunctiveFormula formula = new DisjunctiveFormula();
        for (String word : (new Text(text)).getWords()) {
            formula.addLiteral(index.cat(field).cat(word).key());
        }
        formulas.add(formula);
    }

    @SuppressWarnings("unchecked")
    public List<String> run() {
        String tmpkey = index.cat("queries").cat(
                String.valueOf(this.hashCode())).cat("tmp").key();
        String rkey = index.cat("queries").cat(String.valueOf(this.hashCode()))
                .cat("result").key();
        ZParams zparams = new ZParams();
        zparams.aggregate(Aggregate.MAX);
        ShardedJedis jedis = Seek.getPool().getResource();
        Jedis shard = jedis.getShard(shardKey);
        try {
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
            Seek.getPool().returnResource(jedis);
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