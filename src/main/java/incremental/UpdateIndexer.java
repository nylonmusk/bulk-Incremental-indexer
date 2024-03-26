package incremental;

import config.ElasticConfiguration;
import constant.Update;
import dump.DataReader;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import view.Log;

import java.io.IOException;
import java.util.Map;

public class UpdateIndexer {

    public void update(ElasticConfiguration elasticConfiguration, String index, Map<String, Object> updateConfig) throws IOException {
        final String id = updateConfig.get(Update.ID.get()).toString();
        final String updateDumpPath = updateConfig.get(Update.UPDATE_DUMP_PATH.get()).toString();
        final UpdateRequest request = new UpdateRequest(index, id);
        final DataReader dataReader = new DataReader();

        String json = dataReader.readJsonFileToString(updateDumpPath);
        request.doc(json, XContentType.JSON);
        UpdateResponse updateResponse = elasticConfiguration.getElasticClient().update(request, RequestOptions.DEFAULT);

        if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            Log.info(UpdateIndexer.class.getName(), "Bulk update successful");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            Log.info(UpdateIndexer.class.getName(), "Bulk already updated");
        } else {
            Log.error(UpdateIndexer.class.getName(), "Bulk update failed");
        }
    }
}
