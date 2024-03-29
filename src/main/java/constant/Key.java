package constant;

public enum Key {
    ID("id"),
    TARGET_ID("es_id"),
    DUMP_PATH("dumpPath"),

    SETTINGS("settings"),
    MAPPINGS("mappings"),

    HOST("host"),
    PORT("port"),
    USER("user"),
    PROTOCOL("protocol"),
    INDEX("index");

    private final String keyword;

    Key(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
