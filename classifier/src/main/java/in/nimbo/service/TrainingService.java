package in.nimbo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.entity.Page;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.entity.Category;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class TrainingService {
    private static Logger logger = LoggerFactory.getLogger("classifier");

    public void extractTraining() {

    }

    private static void runSaver() throws IOException {
        ElasticConfig elasticConfig = ElasticConfig.load();
        CopyOnWriteArrayList<Page> backupList = new CopyOnWriteArrayList<>();
        ElasticDAOImpl elasticDAO = ElasticDAOImpl.createElasticDAO(elasticConfig, backupList);
        BulkProcessor bulkProcessor = elasticDAO.getBulkProcessor();
        RestHighLevelClient client = elasticDAO.getClient();

        Map<String, String> list = new HashMap<>();
        try (Scanner scanner = new Scanner(new FileReader("sites.txt"))) {
            while (scanner.hasNextLine()) {
                String[] s = scanner.nextLine().split(" ");
                String link = s[0];
                String cat = s[1];
                list.put(link, cat);
            }
        }

        MultiGetRequest request = new MultiGetRequest();
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        for (Map.Entry<String, String> entry : list.entrySet()) {
            entries.add(entry);
            System.out.println(entries.size());
            if (entries.size() == 2359) {
                System.out.println("start collect");
                MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
                System.out.println("start send");
                MultiGetItemResponse[] responses = response.getResponses();
                for (int i = 0; i < responses.length; i++) {
                    MultiGetItemResponse respons = responses[i];
                    GetResponse firstGet = respons.getResponse();
                    if (firstGet.isExists()) {
                        Map<String, Object> sourceAsMap = firstGet.getSourceAsMap();
                        String content = (String) sourceAsMap.get("content");
                        String l = (String) sourceAsMap.get("link");
                        XContentBuilder builder1 = XContentFactory.jsonBuilder();
                        builder1.startObject();
                        builder1.field("link", l);
                        builder1.field("content", content);
                        builder1.field("labelContent", entries.get(i).getValue());
                        builder1.endObject();
                        IndexRequest indexRequest = new IndexRequest("train", "page", LinkUtility.hashLink(l))
                                .source(builder1);
                        bulkProcessor.add(indexRequest);
                    } else {
                        System.out.println(firstGet.getId() + " not exists");
                    }

                }
                request = new MultiGetRequest();
                entries.clear();
            } else {
                entries.add(entry);
                request.add(new MultiGetRequest.Item("keen", "page", LinkUtility.hashLink(LinkUtility.reverseLink(entry.getKey())))
                        .fetchSourceContext(FetchSourceContext.FETCH_SOURCE));
            }
        }
        System.out.println("finish");
        bulkProcessor.flush();
    }

    private static void runCrawler() throws IOException {
        HBaseConfig hBaseConfig = HBaseConfig.load();
        Connection hBaseConnection = null;
        try {
            hBaseConnection = ConnectionFactory.createConnection();
            logger.info("HBase started");
            System.out.println("hbase started");
        } catch (IOException e) {
            logger.error("Unable to establish HBase connection", e);
            System.exit(1);
        }

        ObjectMapper mapper = new ObjectMapper();
//        List<Category> categories = loadFeed(mapper);
        List<Category> categories = null;
        System.out.println("data loaded");
        try (PrintWriter writer = new PrintWriter("sites.txt", "UTF-8")) {
            try (Table table = hBaseConnection.getTable(TableName.valueOf(hBaseConfig.getPageTable()))) {
                for (Category category : categories) {
                    for (String site : category.getSites()) {
                        String realSite = LinkUtility.reverseLink("https://" + site);
                        System.out.println("start site: " + realSite);
                        Scan s = new Scan(Bytes.toBytes(realSite), Bytes.toBytes(realSite + "0"));
                        ResultScanner scanner = table.getScanner(s);
                        List<String> links = new ArrayList<>();
                        for (Result result : scanner) {
                            links.add(Bytes.toString(result.getRow()));
                        }

                        for (String link : links) {
                            writer.println(link + " " + category.getName());
                        }
                    }
                }
            }
        }
    }
}
