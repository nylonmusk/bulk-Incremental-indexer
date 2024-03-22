package service;

import constant.Keyword;
import validation.ConfigValidator;

import java.util.Map;

public class UpdateConfigService {
    private final Map<String, Object> updateConfig;

    public UpdateConfigService(Map<String, Object> updateConfig) {
        this.updateConfig = updateConfig;
    }

    public String getIndex() {
        if (ConfigValidator.isValid(Keyword.INDEX.get(), updateConfig)) {
            return updateConfig.get(Keyword.INDEX.get()).toString();
        }
        return null;
    }

    public String getColumn() {
        if (ConfigValidator.isValid(Keyword.COLUMN.get(), updateConfig)) {
            return updateConfig.get(Keyword.COLUMN.get()).toString();
        }
        return null;
    }

    public String getTarget() {
        if (ConfigValidator.isValid(Keyword.TARGET.get(), updateConfig)) {
            return updateConfig.get(Keyword.TARGET.get()).toString();
        }
        return null;
    }

    public String getKey() {
        if (ConfigValidator.isValid(Keyword.KEY.get(), updateConfig)) {
            return updateConfig.get(Keyword.KEY.get()).toString();
        }
        return null;
    }
}

