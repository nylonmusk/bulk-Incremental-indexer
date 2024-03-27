package incremental;

import config.ElasticConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class Indexer {
    protected ElasticConfiguration elasticConfiguration;
    protected List<Map<String, Object>> jsonData;
    protected String index;
    protected Map<String, Object> config;

    public Indexer(ElasticConfiguration elasticConfiguration, List<Map<String, Object>> jsonData, String index, Map<String, Object> config) {
        this.elasticConfiguration = elasticConfiguration;
        this.jsonData = jsonData;
        this.index = index;
        this.config = config;
    }

    public abstract void execute() throws IOException;

}
