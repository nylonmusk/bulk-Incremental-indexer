package service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constant.ActionType;
import constant.Keyword;
import validation.ConfigValidator;
import view.Log;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class ConfigService {
    private final Map<String, Map<String, Object>> configData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ConfigService(String filePath) {
        this.configData = loadJsonFromFile(filePath);
    }

    private Map<String, Map<String, Object>> loadJsonFromFile(String filePath) {
        try {
            Map<String, Map<String, Object>> data = objectMapper.readValue(new File(filePath), new TypeReference<Map<String, Map<String, Object>>>() {
            });
            Log.info(ConfigService.class.getName(), "Read Config File successful");
            return data;
        } catch (IOException e) {
            Log.error(ConfigService.class.getName(), "Failed to read config file: " + e.getMessage());
        }
        return Collections.emptyMap();
    }

    public Map<String, Object> getDeleteConfig() {
        if (ConfigValidator.isValid(configData, ActionType.DELETE.get())) {
            return configData.get(ActionType.DELETE.get());
        }
        return Collections.EMPTY_MAP;
    }

    public Map<String, Object> getUpdateConfig() {
        if (ConfigValidator.isValid(configData, ActionType.UPDATE.get())) {
            return configData.get(ActionType.UPDATE.get());
        }
        return Collections.EMPTY_MAP;
    }

    public String getDumpPath() {
        if (ConfigValidator.isValid(configData, ActionType.INSERT.get(), Keyword.DUMP_PATH.get())) {
            return configData.get(ActionType.INSERT.get()).get(Keyword.DUMP_PATH.get()).toString();
        }
        return null;
    }
}
