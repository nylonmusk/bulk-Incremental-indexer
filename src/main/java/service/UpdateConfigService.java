package service;

import constant.Keyword;

import java.util.Map;

public class UpdateConfigService {
    private final Map<String, Object> updateConfig;

    public UpdateConfigService(Map<String, Object> updateConfig) {
        this.updateConfig = updateConfig;
    }

    public String getIndex() {
        return updateConfig.get(Keyword.INDEX.get()).toString();
    }

    public String getColumn() {
        return updateConfig.get(Keyword.COLUMN.get()).toString();
    }

    public String getTarget() {
        return updateConfig.get(Keyword.TARGET.get()).toString();
    }

    public String getKey() {
        return updateConfig.get(Keyword.KEY.get()).toString();
    }
}

