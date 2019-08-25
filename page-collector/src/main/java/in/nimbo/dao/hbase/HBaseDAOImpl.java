package in.nimbo.dao.hbase;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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

            for (Meta meta : page.getMetas()) {
                put.addColumn(config.getMetaColumnFamily(),
                        Bytes.toBytes(meta.getKey()), Bytes.toBytes(meta.getContent()));
            }

            put.addColumn(config.getRankColumnFamily(), config.getRankColumn(), Bytes.toBytes("1"));

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
            Result result = table.get(get);
            return !result.isEmpty();
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
