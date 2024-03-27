package incremental;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.ElasticConfiguration;
import constant.Key;
import dump.DataReader;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
    public void execute() throws IOException {
        final String updateId = getId();
        final String dumpPath = getDumpPath();
        final DataReader dataReader = new DataReader();
        String json = dataReader.readJsonFileToString(dumpPath);
        List<Map<String, Object>> jsonData = new ObjectMapper().readValue(json, List.class);

        for (Map<String, Object> data : jsonData) {
            final String id = data.get(updateId).toString();
            GetRequest getRequest = new GetRequest(index, id);
            GetResponse getResponse = elasticConfiguration.getElasticClient().get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();

                if (sourceAsMap.get("es_id").toString().equals(id)) {
                    String updatedJson = new ObjectMapper().writeValueAsString(data);
                    UpdateRequest request = new UpdateRequest(index, id).doc(updatedJson, XContentType.JSON);
                    UpdateResponse updateResponse = elasticConfiguration.getElasticClient().update(request, RequestOptions.DEFAULT);
                    updateResponse.setForcedRefresh(true);

                    if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                        Log.info(UpdateIndexer.class.getName(), "Bulk update successful with ID : " + id);
                    } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                        Log.info(UpdateIndexer.class.getName(), "Bulk already updated with ID : " + id);
                    } else {
                        Log.error(UpdateIndexer.class.getName(), "Bulk update failed with ID : " + id + " " + updateResponse.getResult());
                    }
                }
            }
        }
    }

    private String getId() {
        return config.get(Key.ID.get()).toString();
    }

    private String getDumpPath() {
        return config.get(Key.DUMP_PATH.get()).toString();
    }
}
