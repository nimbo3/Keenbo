package in.nimbo.dao.hbase;

import in.nimbo.config.HBaseConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.service.ParserService;
import in.nimbo.utility.LinkUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class HBaseDAOImpl implements HBaseDAO {
    private Logger logger = LoggerFactory.getLogger(HBaseDAOImpl.class);
    private HBaseConfig config;
    private Connection connection;

    public HBaseDAOImpl(Connection connection, HBaseConfig config) {
        this.connection = connection;
        this.config = config;
    }

    public void close() throws IOException {
        connection.close();
    }

    @Override
    public boolean contains(String link) {
        try (Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            Get get = new Get(Bytes.toBytes(link));
            Result result = table.get(get);
            return !result.isEmpty();
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public boolean add(Page page) {
        try (Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            Put put = new Put(Bytes.toBytes(page.getReversedLink()));

            put.addColumn(config.getContentColumnFamily(),
                    config.getContentColumn(), Bytes.toBytes(page.getContentWithTags()));

            for (Anchor anchor : page.getAnchors()) {
                put.addColumn(config.getAnchorColumnFamily(),
                        Bytes.toBytes(anchor.getHref()), Bytes.toBytes(anchor.getContent()));
            }

            for (Meta meta : page.getMetas()) {
                put.addColumn(config.getMetaColumnFamily(),
                        Bytes.toBytes(meta.getKey()), Bytes.toBytes(meta.getContent()));
            }

            table.put(put);
            return true;
        } catch (IllegalArgumentException e) {
            // It will be thrown if size of page will be more than hbase.client.keyvalue.maxsize = 10485760
            return false;
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public void syncWithElastic(ElasticDAO elasticDAO, ParserService parserService) {
        logger.info("Start syncing with elasticsearch");
        try (Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            Scan scan = new Scan();
            scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(config.getContentColumnFamily())));
            try (ResultScanner scanner = table.getScanner(scan)){
                logger.info("Start iterating with scanner");
                long count = 0;
                for (Result result : scanner) {
                    String link = LinkUtility.reverseLink(Bytes.toString(result.getRow()));
                    String content = Bytes.toString(result.getValue(config.getContentColumnFamily(),
                            config.getContentColumn()));
                    try {
                        Document document = Jsoup.parse(content);
                        String pageContentWithoutTag = document.text().replace("\n", " ");
                        List<Meta> metas = parserService.getMetas(document);
                        String title = parserService.getTitle(document);
                        Page page = new Page(link, title, "", pageContentWithoutTag, new HashSet<>(), metas, 1.0);
                        elasticDAO.save(page);
                        count++;
                        if (count % 1000 == 0)
                            logger.info("Adding 1000 page to ElasticSearch. Totals: {}", count);
                    } catch (Exception e) {
                        logger.warn("Unable to parse link {} with content {}", link,
                                content.substring(0, content.length() > 10 ? 10 : content.length()));
                    }
                }
                logger.info("finish iterating with scanner");
            }
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
