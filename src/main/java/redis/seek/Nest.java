package redis.seek;


public class Nest {
    private static final String COLON = ":";
    private StringBuilder sb;
    private String key;

    public Nest(String key) {
        this.key = key;
    }

    public Nest fork() {
        return new Nest(key());
    }

    public String key() {
        prefix();
        String generatedKey = sb.toString();
        generatedKey = generatedKey.substring(0, generatedKey.length() - 1);
        sb = null;
        return generatedKey;
    }

    private void prefix() {
        if (sb == null) {
            sb = new StringBuilder();
            sb.append(key);
            sb.append(COLON);
        }
    }

    public Nest cat(int id) {
        prefix();
        sb.append(id);
        sb.append(COLON);
        return this;
    }

    public Nest cat(String field) {
        prefix();
        sb.append(field);
        sb.append(COLON);
        return this;
    }
}