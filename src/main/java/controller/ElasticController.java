package controller;


import bulk.BulkIndexer;
import config.ElasticConfiguration;
import constant.ActionType;
import constant.Server;
import dump.DataReader;
import incremental.DeleteIndexer;
import incremental.PutIndexer;
import incremental.UpdateIndexer;
import service.ConfigService;
import view.Log;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ElasticController {
    private ConfigService configService;
    private DataReader dataReader = new DataReader();

    public ElasticController(String configFilePath) {
        this.configService = new ConfigService(configFilePath);
    }

    public void execute() {
        final String HOST = configService.getServerConfig(Server.HOST).toString();
        final int PORT = (int) configService.getServerConfig(Server.PORT);
        final String USER = configService.getServerConfig(Server.USER).toString();
        final String PROTOCOL = configService.getServerConfig(Server.PROTOCOL).toString();
        final String INDEX = configService.getServerConfig(Server.INDEX).toString();

        try (ElasticConfiguration elasticConfiguration = new ElasticConfiguration(HOST, PORT, USER, PROTOCOL)) {

            if (configService.hasActionType(ActionType.PUT)) put(elasticConfiguration, INDEX);
            if (configService.hasActionType(ActionType.INSERT)) insert(elasticConfiguration, INDEX);
            if (configService.hasActionType(ActionType.UPDATE)) update(elasticConfiguration, INDEX);
            if (configService.hasActionType(ActionType.DELETE)) delete(elasticConfiguration, INDEX);

        } catch (IOException e) {
            Log.error(ElasticController.class.getName(), "execute failed");
        }
    }

    private void put(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        List<Map<String, Object>> jsonData = dataReader.readJsonFileToList(configService.getDumpPath(ActionType.PUT));
        BulkIndexer bulkIndexer = new BulkIndexer(configService.getPutConfig(), jsonData);
        bulkIndexer.put(elasticConfiguration, index);
    }

    private void insert(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        List<Map<String, Object>> jsonData = dataReader.readJsonFileToList(configService.getDumpPath(ActionType.INSERT));
        PutIndexer putIndexer = new PutIndexer(jsonData);
        putIndexer.put(elasticConfiguration, index, jsonData);
    }

    private void update(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        UpdateIndexer updateIndexer = new UpdateIndexer();
        updateIndexer.update(elasticConfiguration, index, configService.getUpdateConfig());
    }

    private void delete(ElasticConfiguration elasticConfiguration, String index) throws IOException {
        DeleteIndexer deleteIndexer = new DeleteIndexer();
        deleteIndexer.delete(elasticConfiguration, index, configService.getDeleteConfig());
    }
}
