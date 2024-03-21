package service;

import constant.Keyword;
import validation.ConfigValidator;

import java.util.Map;

public class DeleteConfigService {
    private final Map<String, Object> deleteConfig;

    public DeleteConfigService(Map<String, Object> deleteConfig) {
        this.deleteConfig = deleteConfig;
    }

    public String getIndex() {
        if (ConfigValidator.isValid(Keyword.INDEX.get(), deleteConfig)) {
            return deleteConfig.get(Keyword.INDEX.get()).toString();
        }
        return null;
    }

    public String getColumn() {
        if (ConfigValidator.isValid(Keyword.COLUMN.get(), deleteConfig)) {
            return deleteConfig.get(Keyword.COLUMN.get()).toString();
        }
        return null;
    }

    public String getTarget() {
        if (ConfigValidator.isValid(Keyword.TARGET.get(), deleteConfig)) {
            return deleteConfig.get(Keyword.TARGET.get()).toString();
        }
        return null;
    }
}
