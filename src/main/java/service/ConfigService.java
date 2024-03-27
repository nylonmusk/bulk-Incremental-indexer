package service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constant.Key;
import constant.Type;
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

    public boolean hasActionType(Type type) {
        return configData.containsKey(type.get());
    }

    public Object getConfig(Key key) {
        if (ConfigValidator.isValid(configData, Type.SERVER.get())) {
            return configData.get(Type.SERVER.get()).get(key.get());
        }
        return null;
    }

    public Map<String, Object> getConfig(Type type) {
        if (ConfigValidator.isValid(configData, type.get())) {
            return configData.get(type.get());
        }
        return Collections.EMPTY_MAP;
    }

    public String getDumpPath(Type type) {
        if (ConfigValidator.isValid(configData, type.get(), Key.DUMP_PATH.get())) {
            return configData.get(type.get()).get(Key.DUMP_PATH.get()).toString();
        }
        return null;
    }
}
