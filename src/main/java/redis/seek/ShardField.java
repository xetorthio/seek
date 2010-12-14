package redis.seek;

public class ShardField {
    private String field;
    private String value;

    public ShardField(String field, String value) {
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
