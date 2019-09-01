package in.nimbo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.config.SparkConfig;
import in.nimbo.controller.AuthController;
import in.nimbo.controller.GraphController;
import in.nimbo.controller.SearchController;
import in.nimbo.dao.auth.AuthDAO;
import in.nimbo.dao.auth.MySqlAuthDAO;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.dao.hbase.HBaseDAO;
import in.nimbo.common.dao.hbase.HBaseDAOImpl;
import in.nimbo.dao.redis.LabelDAO;
import in.nimbo.dao.redis.RedisLabelDAO;
import in.nimbo.entity.Page;
import in.nimbo.entity.User;
import in.nimbo.transformer.JsonTransformer;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import spark.Spark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class App {
    private static Logger backendLogger = LoggerFactory.getLogger("backend");
    private static Logger appLogger = LoggerFactory.getLogger("cli");
    private SearchController searchController;
    private GraphController graphController;
    private SparkConfig sparkConfig;
    private JsonTransformer transformer;
    private RestHighLevelClient client;
    private AuthController authController;
    private AuthDAO authDAO;
    private Connection connection;

    App(SearchController searchController, GraphController graphController, SparkConfig sparkConfig, JsonTransformer transformer, RestHighLevelClient client, AuthController authController, AuthDAO authDAO, Connection connection) {
        this.searchController = searchController;
        this.graphController = graphController;
        this.sparkConfig = sparkConfig;
        this.transformer = transformer;
        this.client = client;
        this.authController = authController;
        this.authDAO = authDAO;
        this.connection = connection;
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        backendLogger.info("loading configurations");
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writer();
        JsonTransformer transformer = new JsonTransformer(writer);
        ElasticConfig elasticConfig = ElasticConfig.load();
        SparkConfig sparkConfig = SparkConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();
        backendLogger.info("configurations loaded");

        Random random = new Random();

        backendLogger.info("connecting to databases");
        Class.forName(sparkConfig.getDatabaseDriver());
        Connection mySqlConnection = DriverManager.getConnection(sparkConfig.getDatabaseURL(), sparkConfig.getDatabaseUser(), sparkConfig.getDatabasePassword());
        RestHighLevelClient restHighLevelClient = initializeElasticSearchClient(elasticConfig);
        Jedis jedis = new Jedis();
        org.apache.hadoop.hbase.client.Connection hBaseConnection = ConnectionFactory.createConnection();
        backendLogger.info("connected to databases");

        backendLogger.info("initializing application");
        AuthDAO authDAO = new MySqlAuthDAO(mySqlConnection);
        ElasticDAO elasticDAO = new ElasticDAOImpl(restHighLevelClient, elasticConfig);
        LabelDAO labelDAO = new RedisLabelDAO(jedis, sparkConfig);
        HBaseDAO hBaseDAO = new HBaseDAOImpl(hBaseConnection, hBaseConfig);

        SearchController searchController = new SearchController(elasticDAO, labelDAO);
        AuthController authController = new AuthController(authDAO, sparkConfig, random, labelDAO);
        GraphController graphController = new GraphController(hBaseDAO, hBaseConfig, sparkConfig, mapper);

        App app = new App(searchController, graphController, sparkConfig, transformer, restHighLevelClient, authController, authDAO, mySqlConnection);

        app.initSpark();
        app.startApp();
    }

    private void initSpark() {
        Spark.port(sparkConfig.getPort());
        Spark.path("/", () -> {
            Spark.before("/*", (request, response) -> backendLogger.info("New request for uri: {}", request.uri()));

            Spark.get("/search", ((request, response) -> {
                String query = request.queryParamOrDefault("query", "");
                List<Page> result = searchController.search(query);
                response.type("application/json");
                return result;
            }), transformer);

            Spark.post("/alert", (request, response) -> {
                Process process = Runtime.getRuntime().exec("/root/send.sh");
                Scanner scanner = new Scanner(process.getInputStream());
                while (scanner.hasNextLine()) {
                    System.out.println(scanner.nextLine());
                }
                return "ok";
            });

            Spark.post("/auth/login", ((request, response) -> {
                String username = request.queryParamOrDefault("username", "");
                String password = request.queryParamOrDefault("password", "");
                response.type("application/json");
                return authController.login(username, password);
            }), transformer);

            Spark.post("/auth/register", ((request, response) -> {
                String username = request.queryParams("username");
                String password = request.queryParams("password");
                String confirmPass = request.queryParams("re_password");
                String email = request.queryParams("email");
                String name = request.queryParams("name");
                response.type("application/json");
                return authController.register(username, password, confirmPass, email, name);
            }), transformer);

            Spark.post("/action/click", ((request, response) -> {
                String token = request.queryParamOrDefault("token", "");
                String destination = request.queryParamOrDefault("dest","");
                User user = authDAO.authenticate(token);
                response.type("application/json");
                return authController.click(user, destination);
            }), transformer);

            Spark.get("/site-graph", (request, response) -> {
                String link = request.queryParamOrDefault("link", "");
                response.type("application/json");
                return graphController.siteGraph(link);
            }, transformer);

            Spark.get("/word-graph", (request, response) -> {
                response.type("application/json");
                return graphController.wordGraph();
            }, transformer);

            Spark.exception(Exception.class, (e, request, response) -> {
                backendLogger.error(e.getMessage(), e);
                response.type("text/html");
                response.status(500);
            });

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
                    connection.close();
                    client.close();
                    Spark.stop();
                    break;
                } catch (IOException | SQLException e) {
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
