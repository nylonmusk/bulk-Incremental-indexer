import bulk.BulkIndexer;
import controller.ElasticController;
import dump.DataReader;
import incremental.IncrementalIndexer;
import service.ConfigService;
import service.DeleteConfigService;
import service.UpdateConfigService;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        ConfigService configService = new ConfigService("C:\\Users\\mayfarm\\Documents\\indexerConfig.json");

        ElasticController elasticController = ElasticController.builder()
                .withDataReader(new DataReader())
                .withBulkIndexer(new BulkIndexer())
                .withIncrementalIndexer(new IncrementalIndexer())
                .withConfigService(configService)
                .withDeleteConfigService(new DeleteConfigService(configService.getDeleteConfig()))
                .withUpdateConfigService(new UpdateConfigService(configService.getUpdateConfig()))
                .build();
        elasticController.execute();
    }
}
