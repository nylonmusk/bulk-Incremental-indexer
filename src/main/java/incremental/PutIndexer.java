package incremental;

import config.ElasticConfiguration;
import constant.Key;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PutIndexer extends Indexer {

    public PutIndexer(ElasticConfiguration elasticConfiguration, List<Map<String, Object>> jsonData, String index, Map<String, Object> putConfig) {
        super(elasticConfiguration, jsonData, index, putConfig);
    }

    @Override
    public void execute() throws IOException {

        if (indexExists(elasticConfiguration, index)) {
            elasticConfiguration.getElasticClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
        }

        final BulkRequest bulkRequest = new BulkRequest();
        final String id = getId();
        final Map<String, Object> settings = getSettings();
        final Map<String, Object> mappings = getMappings();

        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settings);
        createIndexRequest.mapping(mappings);

        for (Map<String, Object> data : jsonData) {
            IndexRequest indexRequest = new IndexRequest(index)
                    .id(data.get(id).toString())
                    .source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = elasticConfiguration.getElasticClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        if (bulkResponse.hasFailures()) {
            Log.info(PutIndexer.class.getName(), "Bulk put failed: " + bulkResponse.buildFailureMessage());
        } else {
            Log.info(PutIndexer.class.getName(), "Bulk put successful");
        }
    }

    private String getId() {
        return config.get(Key.ID.get()).toString();
    }

    private Map<String, Object> getSettings() {
        return (Map<String, Object>) config.get(Key.SETTINGS.get());
    }

    private Map<String, Object> getMappings() {
        return (Map<String, Object>) config.get(Key.MAPPINGS.get());
    }

    private boolean indexExists(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        return elasticConfiguration.getElasticClient().indices().exists(new org.elasticsearch.client.indices.GetIndexRequest(index), RequestOptions.DEFAULT);
    }
}
