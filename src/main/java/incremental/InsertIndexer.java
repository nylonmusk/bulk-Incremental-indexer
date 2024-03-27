package incremental;

import config.ElasticConfiguration;
import constant.Key;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InsertIndexer extends Indexer {

    public InsertIndexer(ElasticConfiguration elasticConfiguration, List<Map<String, Object>> jsonData, String index, Map<String, Object> insertConfig) {
        super(elasticConfiguration, jsonData, index, insertConfig);
    }

    @Override
    public void execute() throws IOException {
        final String id = getId();
        final BulkRequest bulkRequest = new BulkRequest();

        for (Map<String, Object> data : jsonData) {
            IndexRequest indexRequest = new IndexRequest(index)
                    .id(data.get(id).toString())
                    .source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        try {
            BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            if (bulkResponse.hasFailures()) {
                Log.error(InsertIndexer.class.getName(), "Bulk insert failed: " + bulkResponse.buildFailureMessage());
            } else {
                Log.info(InsertIndexer.class.getName(), "Bulk insert successful");
            }
        } catch (IndexNotFoundException e) {
            Log.error(InsertIndexer.class.getName(), "Index not found: " + index);
        }
    }

    private String getId() {
        return config.get(Key.ID.get()).toString();
    }
}
