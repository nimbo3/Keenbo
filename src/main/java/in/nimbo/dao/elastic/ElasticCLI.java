package in.nimbo.dao.elastic;

import in.nimbo.config.ElasticConfig;
import in.nimbo.entity.Page;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Scanner;

public class ElasticCLI {
    public static void main(String[] args) {
        ElasticConfig elasticConfig = ElasticConfig.load();
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort())));
        ElasticDAO elasticDAO = new ElasticDAOImpl(restHighLevelClient, elasticConfig);
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print("cmd> ");
            String type = input.next();
            switch (type) {
                case "getAllPages":
                    List<Page> allPages = elasticDAO.getAllPages();
                    for (Page page : allPages) {
                        System.out.println("link: " + page.getLink());
                        System.out.println("title: " + page.getTitle());
                        System.out.println("----------");
                    }
                    break;
                case "search":
                    String query = input.next();
                    List<Page> pages = elasticDAO.search(query);
                    for (Page page : pages) {
                        System.out.println("title: " + page.getTitle());
                        System.out.println("link: " + page.getLink());
                        System.out.println("----------");
                    }
                    break;
                case "exit":
                    System.out.println("Good bye");
                    System.exit(0);
                default:
                    System.out.println("Invalid input");
                    break;
            }
        }
    }
}
