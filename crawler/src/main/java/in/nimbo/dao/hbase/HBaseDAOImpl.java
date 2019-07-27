package in.nimbo.dao.hbase;

import in.nimbo.config.HBaseConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.service.ParserService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HBaseDAOImpl implements HBaseDAO {
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
        try (Table table = connection.getTable(TableName.valueOf("test"))) {
            Scan scan = new Scan();
            scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(config.getContentColumnFamily())));
            try (ResultScanner scanner = table.getScanner(scan)){
                for (Result result : scanner) {
                    String link = Bytes.toString(result.getRow());
                    String content = Bytes.toString(result.getValue(config.getContentColumnFamily(),
                            config.getContentColumn()));
                    Document document = Jsoup.parse(content);
                    String pageContentWithoutTag = document.text().replace("\n", " ");
                    Set<Anchor> anchors = parserService.getAnchors(document);
                    List<Meta> metas = parserService.getMetas(document);
                    String title = parserService.getTitle(document);
                    Page page = new Page(link, title, "", pageContentWithoutTag, anchors, metas, 1.0);
                    elasticDAO.save(page);
                }
            }
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
