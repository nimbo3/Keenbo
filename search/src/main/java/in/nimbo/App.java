package in.nimbo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.config.SparkConfig;
import in.nimbo.controller.SearchController;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.entity.Page;
import in.nimbo.transformer.JsonTransformer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class App {
    private static Logger backendLogger = LoggerFactory.getLogger("backend");
    private static Logger appLogger = LoggerFactory.getLogger("stdout");
    private SearchController searchController;
    private SparkConfig sparkConfig;
    private JsonTransformer transformer;
    private RestHighLevelClient client;

    App(SearchController searchController, SparkConfig sparkConfig, JsonTransformer transformer, RestHighLevelClient client) {
        this.searchController = searchController;
        this.sparkConfig = sparkConfig;
        this.transformer = transformer;
        this.client = client;
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writer();
        JsonTransformer transformer = new JsonTransformer(writer);
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("violence-words.txt");
        Scanner scanner = new Scanner(inputStream);
        List<String> violenceWords = new ArrayList<>();
        while (scanner.hasNextLine()) {
            violenceWords.addAll(Arrays.asList(scanner.nextLine().split(" ")));
        }

        ElasticConfig elasticConfig = ElasticConfig.load();
        SparkConfig sparkConfig = SparkConfig.load();

        RestHighLevelClient restHighLevelClient = initializeElasticSearchClient(elasticConfig);
        ElasticDAO elasticDAO = new ElasticDAOImpl(restHighLevelClient, elasticConfig, violenceWords);
        SearchController searchController = new SearchController(elasticDAO);

        App app = new App(searchController, sparkConfig, transformer, restHighLevelClient);

        app.initSpark();
        app.startApp();
    }

    private void initSpark() {
        Spark.port(sparkConfig.getPort());
        Spark.path("/", () -> {
            Spark.before("/*", (request, response) -> backendLogger.info("New request for uri: {}", request.uri()));
            Spark.get("/search", ((request, response) -> {
                String query = request.queryParams("query");
                List<Page> result = searchController.search(query != null ? query : "");
                response.type("application/json");
                return result;
            }), transformer);
            Spark.after("/*", (request, response) -> {
                response.header("Access-Control-Allow-Origin", "*");
                backendLogger.info("Response sent successfully: {}", request.uri());
            });
        });
    }

    private void startApp() {
        appLogger.info("Application started\n");
        appLogger.info("To stop application, type \'exit\'\n");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String command = scanner.nextLine();
            if (command.equals("exit")) {
                try {
                    client.close();
                    Spark.stop();
                } catch (IOException e) {
                    backendLogger.error("Unable to close resources properly.", e);
                }
            }
        }
    }

    public static RestHighLevelClient initializeElasticSearchClient(ElasticConfig elasticConfig) {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort()))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(elasticConfig.getConnectTimeout())
                        .setSocketTimeout(elasticConfig.getSocketTimeout()))
                .setMaxRetryTimeoutMillis(elasticConfig.getMaxRetryTimeoutMillis());
        return new RestHighLevelClient(restClientBuilder);
    }
}
