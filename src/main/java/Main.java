import controller.ElasticController;

public class Main {
    public static void main(String[] args) {
        final String configFilePath = args[0];

        ElasticController elasticController = new ElasticController(configFilePath);
        elasticController.execute();
    }
}
