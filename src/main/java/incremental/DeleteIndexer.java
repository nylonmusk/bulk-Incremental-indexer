package incremental;

import config.ElasticConfiguration;
import constant.Key;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DeleteIndexer extends Indexer {

    public DeleteIndexer(ElasticConfiguration elasticConfiguration, List<Map<String, Object>> jsonData, String index, Map<String, Object> deleteConfig) {
        super(elasticConfiguration, jsonData, index, deleteConfig);
    }

    @Override
    public void execute() {
        try {
            final String deleteId = getId();

            for (Map<String, Object> data : jsonData) {
                final String id = data.get(deleteId).toString();
                final DeleteRequest request = new DeleteRequest(index, id);
                DeleteResponse deleteResponse = elasticConfiguration.getElasticClient().delete(request, RequestOptions.DEFAULT);
                deleteResponse.setForcedRefresh(true);
                handleDeleteResponse(deleteResponse, id);
            }
        } catch (IOException e) {
            Log.error(DeleteIndexer.class.getName(), "IOException occurred: " + e.getMessage());
        }
    }

    private static void handleDeleteResponse(DeleteResponse deleteResponse, String id) {
        if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED || deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            Log.info(DeleteIndexer.class.getName(), "Bulk delete successful with ID : " + " " + id);
        } else {
            Log.error(DeleteIndexer.class.getName(), "Bulk delete failed with ID : " + " " + id);
        }
    }

    private String getId() {
        return config.get(Key.ID.get()).toString();
    }
}
