package redis.seek;

import java.util.List;

public class Result {
    private long count;
    private List<String> ids;
    private double time;

    public double getTime() {
        return time;
    }

    public long getTotalCount() {
        return count;
    }

    public List<String> getIds() {
        return ids;
    }

    public Result(long count, List<String> ids, double time) {
        super();
        this.count = count;
        this.ids = ids;
        this.time = time;
    }
}