package constant;

public enum Put {
    SETTINGS("settings"),
    MAPPINGS("mappings"),
    DUMP_PATH("dumpPath");

    private final String keyword;

    Put(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
