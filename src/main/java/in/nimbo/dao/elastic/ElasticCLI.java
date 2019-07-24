package in.nimbo.dao.elastic;

import in.nimbo.config.ElasticConfig;

import java.util.Scanner;

public class ElasticCLI {
    public static void main(String[] args) {
        ElasticDAO elasticDAO = new ElasticDAOImpl(ElasticConfig.load());
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String type = scanner.next();
            switch (type) {
                case "getAllLinks":
                    System.out.println(elasticDAO.getAllLinks());
                    break;
                case "get": {
                    String link = scanner.next();
                    System.out.println(elasticDAO.get(link));
                    break;
                }
                case "save": {
                    String link = scanner.next();
                    String text = scanner.next();
//                    elasticDAO.save(link, text);
                    break;
                }
                case "exit":
                    System.exit(0);
                default:
                    System.out.println("invalid input");
                    break;
            }
        }
    }
}