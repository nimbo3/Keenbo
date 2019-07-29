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
}
