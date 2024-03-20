package constant;

public enum Keyword {
    INDEX("index"),
    COLUMN("column"),
    TARGET("target"),
    KEY("key"),
    DUMP_PATH("dumpPath");

    private final String keyword;

    Keyword(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
