package redis.seek;

public class Seek {

    public Index index(String index) {
        return new Index(index);
    }

    public Search search(String index, String order) {
        return new Search(index, order);
    }

    public Search search(String index, String order, Shard... shardFields) {
        return new Search(index, order, shardFields);
    }

}
