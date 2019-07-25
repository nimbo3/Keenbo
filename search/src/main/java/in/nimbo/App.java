package in.nimbo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import in.nimbo.config.ElasticConfig;
import in.nimbo.config.SparkConfig;
import in.nimbo.controller.SearchController;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.transformer.JsonTransformer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.io.IOException;
import java.util.Scanner;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writer();
        JsonTransformer transformer = new JsonTransformer(mapper, writer);

        ElasticConfig elasticConfig = ElasticConfig.load();
        SparkConfig sparkConfig = SparkConfig.load();

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort())));
        ElasticDAOImpl elasticDAO = new ElasticDAOImpl(restHighLevelClient, elasticConfig);
        SearchController searchController = new SearchController(elasticDAO);

        Spark.port(sparkConfig.getPort());
        Spark.path("/", () -> {
            Spark.before("/*", (request, response) -> {
                logger.info("new request for uri: " + request.uri());
            });
            Spark.get("/search",((request, response) -> {
                String query = request.queryParams("query");
                return elasticDAO.customSearch(query);
//                return searchController.search(query != null ? query : "");
            }) , transformer);
            Spark.after("/*", (request, response) -> logger.info("response sent successfully: " + request.uri()));
        });

        outer: while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] split = line.split(" ");
            if (split.length > 0){
                switch (split[0]) {
                    case "exit":
                        try {
                            restHighLevelClient.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Spark.stop();
                        break outer;
                }
            }
        }
    }
}
