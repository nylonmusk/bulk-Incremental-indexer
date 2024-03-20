package service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constant.ActionType;
import constant.Keyword;
import view.Log;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ConfigService {
    private Map<String, Map<String, Object>> configData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ConfigService(String filePath) throws IOException {
        this.configData = loadJsonFromFile(filePath);
    }

    private Map<String, Map<String, Object>> loadJsonFromFile(String filePath) throws IOException {
        configData = objectMapper.readValue(new File(filePath), new TypeReference<Map<String, Map<String, Object>>>() {
        });
        Log.info(ConfigService.class.getName(), "read Config File.");
        return configData;
    }

    public Map<String, Object> getDeleteConfig() {
        return configData.get(ActionType.DELETE.get());
    }

    public Map<String, Object> getUpdateConfig() {
        return configData.get(ActionType.UPDATE.get());
    }

    public String getDumpPath() {
        return configData.get(ActionType.INSERT.get()).get(Keyword.DUMP_PATH.get()).toString();
    }
}
