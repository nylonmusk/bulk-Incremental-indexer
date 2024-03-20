package service;

import constant.Keyword;

import java.util.Map;

public class DeleteConfigService {
    private final Map<String, Object> deleteConfig;

    public DeleteConfigService(Map<String, Object> deleteConfig) {
        this.deleteConfig = deleteConfig;
    }

    public String getIndex() {
        return deleteConfig.get(Keyword.INDEX.get()).toString();
    }

    public String getColumn() {
        return deleteConfig.get(Keyword.COLUMN.get()).toString();
    }

    public String getTarget() {
        return deleteConfig.get(Keyword.TARGET.get()).toString();
    }
}
