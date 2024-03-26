package bulk;

import config.ElasticConfiguration;
import constant.Put;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BulkIndexer {
    Map<String, Object> putConfig;
    List<Map<String, Object>> jsonData;

    public BulkIndexer(Map<String, Object> putConfig, List<Map<String, Object>> jsonData) {
        this.putConfig = putConfig;
        this.jsonData = jsonData;
    }

    public void put(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        if (indexExists(elasticConfiguration, index)) {
            elasticConfiguration.getElasticClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
        }

        final BulkRequest bulkRequest = new BulkRequest();
        final Map<String, Object> settings = getSettings();
        final Map<String, Object> mappings = getMappings();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settings);
        createIndexRequest.mapping(mappings);

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

    private Map<String, Object> getSettings() {
        return (Map<String, Object>) putConfig.get(Put.SETTINGS.get());
    }

    private Map<String, Object> getMappings() {
        return (Map<String, Object>) putConfig.get(Put.MAPPINGS.get());
    }

    private Map<String, Object> getDump() {
        return (Map<String, Object>) putConfig.get(Put.DUMP_PATH.get());
    }

    private boolean indexExists(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        return elasticConfiguration.getElasticClient().indices().exists(new org.elasticsearch.client.indices.GetIndexRequest(index), RequestOptions.DEFAULT);
    }
}
