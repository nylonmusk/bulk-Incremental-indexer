package controller;

import builder.ElasticControllerBuilder;
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

    private final DataReader dataReader;
    private final BulkIndexer bulkIndexer;
    private final IncrementalIndexer incrementalIndexer;
    private final ConfigService configService;
    private final DeleteConfigService deleteConfigService;
    private final UpdateConfigService updateConfigService;

    public ElasticController(DataReader dataReader,
                             BulkIndexer bulkIndexer,
                             IncrementalIndexer incrementalIndexer,
                             ConfigService configService,
                             DeleteConfigService deleteConfigService,
                             UpdateConfigService updateConfigService) {
        this.dataReader = dataReader;
        this.bulkIndexer = bulkIndexer;
        this.incrementalIndexer = incrementalIndexer;
        this.configService = configService;
        this.deleteConfigService = deleteConfigService;
        this.updateConfigService = updateConfigService;
    }

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

    public static ElasticControllerBuilder builder() {
        return new ElasticControllerBuilder();
    }
}
