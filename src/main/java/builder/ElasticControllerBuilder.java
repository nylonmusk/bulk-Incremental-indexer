package builder;

import bulk.BulkIndexer;
import controller.ElasticController;
import dump.DataReader;
import incremental.IncrementalIndexer;
import service.ConfigService;
import service.DeleteConfigService;
import service.UpdateConfigService;

public class ElasticControllerBuilder {
    private DataReader dataReader;
    private BulkIndexer bulkIndexer;
    private IncrementalIndexer incrementalIndexer;
    private ConfigService configService;
    private DeleteConfigService deleteConfigService;
    private UpdateConfigService updateConfigService;

    public ElasticControllerBuilder withDataReader(DataReader dataReader) {
        this.dataReader = dataReader;
        return this;
    }

    public ElasticControllerBuilder withBulkIndexer(BulkIndexer bulkIndexer) {
        this.bulkIndexer = bulkIndexer;
        return this;
    }

    public ElasticControllerBuilder withIncrementalIndexer(IncrementalIndexer incrementalIndexer) {
        this.incrementalIndexer = incrementalIndexer;
        return this;
    }

    public ElasticControllerBuilder withConfigService(ConfigService configService) {
        this.configService = configService;
        return this;
    }

    public ElasticControllerBuilder withDeleteConfigService(DeleteConfigService deleteConfigService) {
        this.deleteConfigService = deleteConfigService;
        return this;
    }

    public ElasticControllerBuilder withUpdateConfigService(UpdateConfigService updateConfigService) {
        this.updateConfigService = updateConfigService;
        return this;
    }

    public ElasticController build() {
        return new ElasticController(dataReader, bulkIndexer, incrementalIndexer, configService, deleteConfigService, updateConfigService);
    }
}
