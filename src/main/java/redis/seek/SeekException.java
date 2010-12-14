package redis.seek;

public class SeekException extends RuntimeException {
    private static final long serialVersionUID = -7526092235887005858L;

    public SeekException(String msg) {
        super(msg);
    }
}
