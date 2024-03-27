package controller;

import config.ElasticConfiguration;
import constant.Type;
import constant.column.Server;
import dump.DataReader;
import incremental.DeleteIndexer;
import incremental.InsertIndexer;
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
        final String HOST = configService.getConfig(Server.HOST).toString();
        final int PORT = (int) configService.getConfig(Server.PORT);
        final String USER = configService.getConfig(Server.USER).toString();
        final String PROTOCOL = configService.getConfig(Server.PROTOCOL).toString();
        final String INDEX = configService.getConfig(Server.INDEX).toString();

        try (ElasticConfiguration elasticConfiguration = new ElasticConfiguration(HOST, PORT, USER, PROTOCOL)) {

            if (configService.hasActionType(Type.PUT)) execute(elasticConfiguration, INDEX, Type.PUT);
            if (configService.hasActionType(Type.INSERT)) execute(elasticConfiguration, INDEX, Type.INSERT);
            if (configService.hasActionType(Type.UPDATE)) execute(elasticConfiguration, INDEX, Type.UPDATE);
            if (configService.hasActionType(Type.DELETE)) execute(elasticConfiguration, INDEX, Type.DELETE);

        } catch (IOException e) {
            Log.error(ElasticController.class.getName(), "execute failed");
        }
    }

    private void execute(ElasticConfiguration elasticConfiguration, String index, Type type) throws IOException {
        List<Map<String, Object>> jsonData = dataReader.readJsonFileToList(configService.getDumpPath(type));

        if (type == Type.PUT) {
            PutIndexer putIndexer = new PutIndexer(elasticConfiguration, jsonData, index, configService.getConfig(type));
            putIndexer.execute();
            return;
        }

        if (type == Type.INSERT) {
            InsertIndexer insertIndexer = new InsertIndexer(elasticConfiguration, jsonData, index, configService.getConfig(type));
            insertIndexer.execute();
            return;
        }

        if (type == Type.UPDATE) {
            UpdateIndexer updateIndexer = new UpdateIndexer(elasticConfiguration, jsonData, index, configService.getConfig(type));
            updateIndexer.execute();
            return;
        }

        if (type == Type.DELETE) {
            DeleteIndexer deleteIndexer = new DeleteIndexer(elasticConfiguration, jsonData, index, configService.getConfig(type));
            deleteIndexer.execute();
        }
    }
}
