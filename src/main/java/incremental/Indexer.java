package incremental;

import config.ElasticConfiguration;
import view.Log;

import java.io.IOException;

public abstract class Indexer {

    protected ElasticConfiguration elasticConfiguration;

    public Indexer(ElasticConfiguration elasticConfiguration) {
        this.elasticConfiguration = elasticConfiguration;
    }

    public abstract void operate(String index) throws IOException;

    protected void logSuccess(Class<?> clazz, String message) {
        Log.info(clazz.getName(), message);
    }

    protected void logError(Class<?> clazz, String message) {
        Log.error(clazz.getName(), message);
    }
}