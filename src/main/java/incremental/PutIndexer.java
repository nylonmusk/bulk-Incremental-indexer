package incremental;

import config.ElasticConfiguration;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PutIndexer {

    List<Map<String, Object>> jsonData;

    public PutIndexer(List<Map<String, Object>> jsonData) {
        this.jsonData = jsonData;
    }

    public void put(ElasticConfiguration elasticConfiguration, String index, List<Map<String, Object>> jsonData) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> data : jsonData) {
            IndexRequest indexRequest = new IndexRequest(index).source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
            Log.error(PutIndexer.class.getName(), "Bulk insert failed: " + bulkResponse.buildFailureMessage());
        } else {
            Log.info(PutIndexer.class.getName(), "Bulk insert successful");
        }
    }
}
