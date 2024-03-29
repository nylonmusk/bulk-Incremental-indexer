package incremental;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.ElasticConfiguration;
import constant.Key;
import dump.DataReader;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class UpdateIndexer extends Indexer {

    public UpdateIndexer(ElasticConfiguration elasticConfiguration, List<Map<String, Object>> jsonData, String index, Map<String, Object> updateConfig) {
        super(elasticConfiguration, jsonData, index, updateConfig);
    }

    @Override
    public void execute() {
        try {
            updateDocuments();
        } catch (IOException e) {
            Log.error(UpdateIndexer.class.getName(), "IOException occurred: " + e.getMessage());
        }
    }

    private void updateDocuments() throws IOException {
        final String updateId = getId();
        final String dumpPath = getDumpPath();
        final DataReader dataReader = new DataReader();
        final ObjectMapper objectMapper = new ObjectMapper();
        String json = dataReader.readJsonFileToString(dumpPath);
        List<Map<String, Object>> jsonData = objectMapper.readValue(json, List.class);

        for (Map<String, Object> data : jsonData) {
            updateDocument(objectMapper, data, updateId);
        }
    }

    private void updateDocument(ObjectMapper objectMapper, Map<String, Object> data, String updateId) throws IOException {
        final String id = data.get(updateId).toString();
        UpdateRequest request = new UpdateRequest(index, id).doc(objectMapper.writeValueAsString(data), XContentType.JSON);
        UpdateResponse updateResponse = elasticConfiguration.getElasticClient().update(request, RequestOptions.DEFAULT);
        updateResponse.setForcedRefresh(true);
        handleUpdateResponse(updateResponse, id);
    }

    private void handleUpdateResponse(UpdateResponse updateResponse, String id) {
        if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            Log.info(UpdateIndexer.class.getName(), "Bulk update successful with ID : " + id);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            Log.info(UpdateIndexer.class.getName(), "Bulk already updated with ID : " + id);
        } else {
            Log.error(UpdateIndexer.class.getName(), "Bulk update failed with ID : " + id + " " + updateResponse.getResult());
        }
    }

    private String getId() {
        return config.get(Key.ID.get()).toString();
    }

    private String getDumpPath() {
        return config.get(Key.DUMP_PATH.get()).toString();
    }
}
