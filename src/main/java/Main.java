import controller.ElasticController;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        ElasticController elasticController = new ElasticController();
        elasticController.execute();
    }
}
