package constant;

public enum Type {
    SERVER("server"),
    PUT("put"),
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

    private final String type;

    Type(String type) {
        this.type = type;
    }

    public String get() {
        return type;
    }
}
