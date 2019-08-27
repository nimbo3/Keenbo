package in.nimbo.dao.hbase;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.service.KeywordExtractorService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class HBaseDAOImpl implements HBaseDAO {
    private HBasePageConfig config;
    private Connection connection;

    public HBaseDAOImpl(Connection connection, HBasePageConfig config) {
        this.connection = connection;
        this.config = config;
    }

    public void close() throws IOException {
        connection.close();
    }

    @Override
    public boolean add(Page page) {
        try (Table table = connection.getTable(TableName.valueOf(config.getPageTable()))) {
            Put put = new Put(Bytes.toBytes(page.getReversedLink()));

            for (Anchor anchor : page.getAnchors()) {
                put.addColumn(config.getAnchorColumnFamily(),
                        Bytes.toBytes(anchor.getHref()), Bytes.toBytes(anchor.getContent()));
            }

            Map<String, Integer> keywords = KeywordExtractorService.extractKeywords(page.getContent());
            for (Map.Entry<String, Integer> keyword : keywords.entrySet()) {
                put.addColumn(config.getDataColumnFamily(),
                        Bytes.toBytes(keyword.getKey()), Bytes.toBytes(Integer.toString(keyword.getValue())));
            }

            put.addColumn(config.getDataColumnFamily(), config.getRankColumn(), Bytes.toBytes("1"));

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
    public boolean contains(String link) {
        try (Table table = connection.getTable(TableName.valueOf(config.getPageTable()))) {
            Get get = new Get(Bytes.toBytes(link));
            return table.exists(get);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
