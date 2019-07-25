package in.nimbo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import in.nimbo.config.ElasticConfig;
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

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writer();
        JsonTransformer transformer = new JsonTransformer(mapper, writer);
        ElasticConfig elasticConfig = ElasticConfig.load();
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort())));
        ElasticDAO elasticDAO = new ElasticDAOImpl(restHighLevelClient, elasticConfig);
        SearchController searchController = new SearchController(elasticDAO);
//        Spark.port(8080);
        Spark.path("/", () -> {
            Spark.before("/*", (request, response) -> {
                logger.info("new request for uri: " + request.uri());
            });
            Spark.get("/search",((request, response) -> {
                String query = request.queryParams("query");
                return searchController.search(query != null ? query : "");
            }) , transformer);
            Spark.after("/*", (request, response) -> logger.info("response sent successfully: " + request.uri()));
        });
    }
}
