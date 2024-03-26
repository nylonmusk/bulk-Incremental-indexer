package constant;

public enum Server {
    HOST("host"),
    PORT("port"),
    USER("user"),
    PROTOCOL("protocol"),
    INDEX("index");

    private final String keyword;

    Server(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
