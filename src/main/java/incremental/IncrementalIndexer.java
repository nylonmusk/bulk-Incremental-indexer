package incremental;

import config.ElasticConfiguration;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import view.Log;

import java.io.IOException;
import java.util.Collections;
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

    public void update(ElasticConfiguration elasticConfiguration, String index, String column, String target, String key) throws IOException {
        UpdateByQueryRequest updateRequest = new UpdateByQueryRequest(index);
        updateRequest.setQuery(QueryBuilders.matchQuery(column, target));

        Script script = new Script(ScriptType.INLINE, "painless", "if (ctx._source['" + column + "'] == '" + target + "') { ctx._source['" + column + "'] = '" + key + "' }", Collections.emptyMap());
        updateRequest.setScript(script);

        BulkByScrollResponse bulkResponse = elasticConfiguration.getElasticClient().updateByQuery(updateRequest, RequestOptions.DEFAULT);
        List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
        if (!bulkFailures.isEmpty()) {
            for (BulkItemResponse.Failure failure : bulkFailures) {
                Log.error(IncrementalIndexer.class.getName(), "Bulk update failed: " + failure.getMessage());
            }
        } else {
            Log.info(IncrementalIndexer.class.getName(), "Bulk update successful");
        }
    }

    public void delete(ElasticConfiguration elasticConfiguration, String index, String column, String target) throws IOException {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(index);
        deleteRequest.setQuery(QueryBuilders.matchQuery(column, target));

        BulkByScrollResponse bulkResponse = elasticConfiguration.getElasticClient().deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
        List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
        if (!bulkFailures.isEmpty()) {
            for (BulkItemResponse.Failure failure : bulkFailures) {
                Log.error(IncrementalIndexer.class.getName(), "Bulk delete failed: " + failure.getMessage());
            }
        } else {
            Log.info(IncrementalIndexer.class.getName(), "Bulk delete successful");
        }
    }
}