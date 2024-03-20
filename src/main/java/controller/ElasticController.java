package controller;

import bulk.BulkIndexer;
import dump.DataReader;
import config.ElasticConfiguration;
import incremental.IncrementalIndexer;
import service.ConfigService;
import service.DeleteConfigService;
import service.UpdateConfigService;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ElasticController {
    private final String HOST = "localhost";
    private final int PORT = 9200;
    private final String USER = "";
    private final String PROTOCOL = "http";

    private final DataReader dataReader = new DataReader();
    private final BulkIndexer bulkIndexer = new BulkIndexer();
    private final IncrementalIndexer incrementalIndexer = new IncrementalIndexer();
    private final ConfigService configService = new ConfigService("C:\\Users\\mayfarm\\Documents\\indexerConfig.json");
    private final DeleteConfigService deleteConfigService = new DeleteConfigService(configService.getDeleteConfig());
    private final UpdateConfigService updateConfigService = new UpdateConfigService(configService.getUpdateConfig());

    public ElasticController() throws IOException {}

    public void execute() {
        try (ElasticConfiguration elasticConfiguration = new ElasticConfiguration(HOST, PORT, USER, PROTOCOL)) {
            List<Map<String, Object>> jsonData = dataReader.readJsonFile(configService.getDumpPath());
            String INDEX = deleteConfigService.getIndex();

            bulkIndexer.execute(elasticConfiguration, INDEX, jsonData);
            incrementalIndexer.insert(elasticConfiguration, INDEX, jsonData);
            incrementalIndexer.update(elasticConfiguration, INDEX, updateConfigService.getColumn(), updateConfigService.getTarget(), updateConfigService.getKey());
            incrementalIndexer.delete(elasticConfiguration, INDEX, deleteConfigService.getColumn(), deleteConfigService.getTarget());

        } catch (IOException e) {
            Log.error(ElasticController.class.getName(), "execute failed");

        }
    }
}
