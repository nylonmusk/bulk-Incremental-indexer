package incremental;

import config.ElasticConfiguration;
import constant.Delete;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import view.Log;

import java.io.IOException;
import java.util.Map;

public class DeleteIndexer {
    Map<String, Object> deleteConfig;
    ElasticConfiguration elasticConfiguration;
    String index;

    public DeleteIndexer(Map<String, Object> deleteConfig, ElasticConfiguration elasticConfiguration, String index) {
        this.deleteConfig = deleteConfig;
        this.elasticConfiguration = elasticConfiguration;
        this.index = index;
    }

    public void delete() throws IOException {
        String id = deleteConfig.get(Delete.ID.get()).toString();
        DeleteRequest request = new DeleteRequest(index, id);
        DeleteResponse deleteResponse = elasticConfiguration.getElasticClient().delete(request, RequestOptions.DEFAULT);
        deleteResponse.setForcedRefresh(true);
        if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED || deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            Log.info(DeleteIndexer.class.getName(), "Bulk delete successful");
        } else {
            Log.error(DeleteIndexer.class.getName(), "Bulk delete failed");
        }
    }
}
