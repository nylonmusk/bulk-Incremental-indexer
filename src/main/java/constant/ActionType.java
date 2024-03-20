package constant;

public enum ActionType {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

    private final String type;

    ActionType(String type) {
        this.type = type;
    }

    public String get() {
        return type;
    }
}
