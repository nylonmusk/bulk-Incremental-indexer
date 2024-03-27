package incremental;

import config.ElasticConfiguration;
import constant.column.Insert;
import constant.column.Put;
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
    public void execute() throws IOException {
        final String deleteId = getId();

        for (Map<String, Object> data : jsonData) {
            final String id = data.get(deleteId).toString();
            DeleteRequest request = new DeleteRequest(index, id);
            DeleteResponse deleteResponse = elasticConfiguration.getElasticClient().delete(request, RequestOptions.DEFAULT);
            deleteResponse.setForcedRefresh(true);
            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED || deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                Log.info(DeleteIndexer.class.getName(), "Bulk delete successful with ID : " + " " + id);
            } else {
                Log.error(DeleteIndexer.class.getName(), "Bulk delete failed with ID : " + " " + id);
            }
        }
    }

    private String getId() {
        return config.get(Put.ID.get()).toString();
    }

    private String getDumpPath() {
        return config.get(Insert.DUMP_PATH.get()).toString();
    }
}
