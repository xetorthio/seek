package redis.seek;


public class Index {
    private String name;

    public String getName() {
        return name;
    }

    public Index(String name) {
        this.name = name;
    }

    public Entry add(String id) {
        return new Entry(this, id);
    }
}