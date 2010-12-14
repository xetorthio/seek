package redis.seek;

import java.util.List;

public class Result {
    private long count;
    private List<String> ids;

    public long getTotalCount() {
        return count;
    }

    public List<String> getIds() {
        return ids;
    }

    public Result(long count, List<String> ids) {
        super();
        this.count = count;
        this.ids = ids;
    }
}