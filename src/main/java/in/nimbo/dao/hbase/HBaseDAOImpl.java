package in.nimbo.dao.hbase;

import in.nimbo.config.HBaseConfig;
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
    public void add(String link) throws HBaseException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            Put put = new Put(Bytes.toBytes(link));
            put.addColumn(Bytes.toBytes(config.getReferenceCountColumnFamily()),
                    Bytes.toBytes(config.getReferenceCountColumn()), Bytes.toBytes("1"));
            table.put(put);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
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
}
