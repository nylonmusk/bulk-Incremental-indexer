package validation;

import java.util.Map;

public class ConfigValidator {


    public static boolean isValid(Map<String, Map<String, Object>> configData, String type) {
        return configData.get(type) != null && !configData.get(type).isEmpty();
    }

    public static boolean isValid(Map<String, Map<String, Object>> configData, String type, String keyword) {
        return configData.get(type).get(keyword) != null && !configData.get(type).get(keyword).toString().isEmpty();
    }

    public static boolean isValid(String type, Map<String, Object> specificConfigData) {
        return specificConfigData.get(type) != null && !specificConfigData.get(type).toString().isEmpty();
    }
}
