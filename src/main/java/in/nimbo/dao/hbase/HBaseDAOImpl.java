package in.nimbo.dao.hbase;

import in.nimbo.config.HBaseConfig;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDAOImpl implements HBaseDAO {
    private Configuration conf;
    private HBaseConfig config;

    public HBaseDAOImpl(Configuration conf, HBaseConfig config) {
        this.conf = conf;
        this.config = config;
    }

    @Override
    public boolean contains(String link) throws HBaseException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            Get get = new Get(Bytes.toBytes(link));
            Result result = table.get(get);
            return result.size() > 0;
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public void add(Page page) {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            Put put = new Put(Bytes.toBytes(page.getReversedLink()));

            put.addColumn(Bytes.toBytes(config.getContentColumnFamily()),
                    Bytes.toBytes(config.getContentColumn()), Bytes.toBytes(page.getContentWithTags()));

            for (Anchor link : page.getAnchors()) {
                put.addColumn(Bytes.toBytes(config.getAnchorsColumnFamily()),
                        Bytes.toBytes(link.getHref()), Bytes.toBytes(link.getAnchor()));
            }

            for (Meta meta : page.getMetas()) {
                put.addColumn(Bytes.toBytes(config.getMetasColumnFamily()),
                        Bytes.toBytes(meta.getKey()), Bytes.toBytes(meta.getContent()));
            }

            table.put(put);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
