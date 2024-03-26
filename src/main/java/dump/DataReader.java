package dump;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DataReader {
    public List<Map<String, Object>> readJsonFileToList(String filePath) throws IOException {
        String jsonContent = new String(Files.readAllBytes(Paths.get(filePath)));
        return Collections.unmodifiableList(new ObjectMapper().readValue(jsonContent, List.class));
    }

    public String readJsonFileToString(String filePath) throws IOException {
        String jsonContent = new String(Files.readAllBytes(Paths.get(filePath)));
        return jsonContent;
    }
}
