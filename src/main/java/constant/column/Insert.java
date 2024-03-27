package constant.column;

public enum Insert {
    ID("id"),
    DUMP_PATH("dumpPath");

    private final String keyword;

    Insert(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
