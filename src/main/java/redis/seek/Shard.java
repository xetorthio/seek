package redis.seek;

public class Shard {
    private String field;
    private String value;

    public Shard(String field, String value) {
        super();
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public String getValue() {
        return value;
    }
}
