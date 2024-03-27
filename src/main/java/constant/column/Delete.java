package constant.column;

public enum Delete {
    ID("id"),
    DUMP_PATH("dumpPath");

    private final String keyword;

    Delete(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
