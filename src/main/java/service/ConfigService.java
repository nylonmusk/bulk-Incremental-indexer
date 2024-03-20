package service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import view.Log;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConfigService {
    private final String filePath;
    private final Map<String, Object> configData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ConfigService(String filePath) throws IOException {
        this.filePath = filePath;
        this.configData = loadJsonFromFile(filePath);
    }

    private Map<String, Object> loadJsonFromFile(String filePath) throws IOException {
        Map<String, Object> data;
        data = objectMapper.readValue(new File(filePath), new TypeReference<Map<String, Object>>() {
        });
        Log.info(ConfigService.class.getName(), "read Config File.");
        return data;
    }

    public String getActionType() {
        return configData.get("actionType").toString();
    }

    public List<String> getIdList() {
        String id = configData.get("id").toString().replaceAll("\\[", "").replaceAll("]", "");
        List<String> idList = Arrays.asList(id.split(", "));
        return idList;
    }

    public String getKey() {
        return configData.get("key").toString();
    }
}
