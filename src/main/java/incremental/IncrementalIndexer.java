package incremental;

import config.ElasticConfiguration;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IncrementalIndexer {

    public void insert(ElasticConfiguration elasticConfiguration, String index, List<Map<String, Object>> dataList) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> data : dataList) {
            IndexRequest indexRequest = new IndexRequest(index).source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            Log.error(IncrementalIndexer.class.getName(), "Bulk insert failed: " + bulkResponse.buildFailureMessage());
        } else {
            Log.info(IncrementalIndexer.class.getName(), "Bulk insert successful");
        }
    }
}