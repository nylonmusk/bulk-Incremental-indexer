package bulk;

import config.ElasticConfiguration;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BulkIndexer {
    public void execute(ElasticConfiguration elasticConfiguration, String index, List<Map<String, Object>> jsonData) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        
        elasticConfiguration.getElasticClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);


        for (Map<String, Object> data : jsonData) {
            IndexRequest indexRequest = new IndexRequest(index).source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
            Log.info(BulkIndexer.class.getName(), "Bulk insert failed: " + bulkResponse.buildFailureMessage());
        } else {
            Log.info(BulkIndexer.class.getName(), "Bulk insert successful");
        }
    }
}
