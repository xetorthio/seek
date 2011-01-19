package redis.seek;

import java.util.HashMap;

public class Info<R, T> extends HashMap<R, T> {
    private long total;
    private static final long serialVersionUID = -3214532453377914466L;

    public long total() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
