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
    ElasticConfiguration elasticConfiguration;
    String index;

    public PutIndexer(List<Map<String, Object>> jsonData, ElasticConfiguration elasticConfiguration, String index) {
        this.jsonData = jsonData;
        this.elasticConfiguration = elasticConfiguration;
        this.index = index;
    }

    public void put() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> data : jsonData) {
            IndexRequest indexRequest = new IndexRequest(index).source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        if (bulkResponse.hasFailures()) {
            Log.error(PutIndexer.class.getName(), "Bulk insert failed: " + bulkResponse.buildFailureMessage());
        } else {
            Log.info(PutIndexer.class.getName(), "Bulk insert successful");
        }
    }
}
