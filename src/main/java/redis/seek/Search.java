package redis.seek;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.ZParams.Aggregate;
import redis.clients.util.SafeEncoder;
import redis.seek.search.ConjunctiveFormula;
import redis.seek.search.DNF;
import redis.seek.search.DisjunctiveFormula;

public class Search {
    private List<DisjunctiveFormula> formulas = new ArrayList<DisjunctiveFormula>();
    private Nest index;

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

    public Set<Tuple> run() {
        String tmpkey = index.cat("queries").cat(
                String.valueOf(this.hashCode())).cat("tmp").key();
        String rkey = index.cat("queries").cat(String.valueOf(this.hashCode()))
                .cat("result").key();
        ZParams zparams = new ZParams();
        zparams.aggregate(Aggregate.MAX);
        Jedis jedis = Nest.getResource();
        Pipeline pipeline = jedis.pipelined();

        List<ConjunctiveFormula> dnf = DNF.convert(formulas);
        for (ConjunctiveFormula conjunctiveFormula : dnf) {
            String[] keys = conjunctiveFormula.getLiterals().toArray(
                    new String[conjunctiveFormula.getLiterals().size()]);
            pipeline.zinterstore(tmpkey, zparams, keys);
            pipeline.zunionstore(rkey, zparams, tmpkey, rkey);
        }
        pipeline.zrangeWithScores(rkey, 0, 50);
        pipeline.del(rkey, tmpkey);
        List<Object> execute = pipeline.execute();
        Nest.returnResource(jedis);
        return getTupledSet((List<byte[]>) execute.get(execute.size() - 2));
    }

    private Set<Tuple> getTupledSet(List<byte[]> membersWithScores) {
        Set<Tuple> set = new LinkedHashSet<Tuple>();
        Iterator<byte[]> iterator = membersWithScores.iterator();
        while (iterator.hasNext()) {
            set.add(new Tuple(SafeEncoder.encode(iterator.next()), Double
                    .valueOf(SafeEncoder.encode(iterator.next()))));
        }
        return set;
    }
}