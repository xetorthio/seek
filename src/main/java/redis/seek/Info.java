package redis.seek;

import java.util.HashMap;
import java.util.Map;

public class Info extends HashMap<String, Map<String, Long>> {
    private long total;
    private static final long serialVersionUID = -3214532453377914466L;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
