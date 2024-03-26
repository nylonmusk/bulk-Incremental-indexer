package constant;

public enum Delete {
    ID("id");

    private final String keyword;

    Delete(String keyword) {
        this.keyword = keyword;
    }

    public String get() {
        return keyword;
    }
}
