package constant.column;

public enum Update {
    ID("id"),
    DUMP_PATH("dumpPath");

    private final String keyword;

    Update(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
