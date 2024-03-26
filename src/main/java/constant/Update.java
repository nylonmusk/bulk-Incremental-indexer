package constant;

public enum Update {
    ID("id"),
    DUMP_PATH("dumpPath"),
    UPDATE_DUMP_PATH("updatedumpPath");

    private final String keyword;

    Update(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
