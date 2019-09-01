package in.nimbo.dao.hbase;

import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Font;
import in.nimbo.entity.Node;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseDAOImpl implements HBaseDAO {
    private HBaseSiteConfig config;
    private Connection connection;

    public HBaseDAOImpl(HBaseSiteConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }


    @Override
    public Result get(String link) {
        try (Table table = connection.getTable(TableName.valueOf(config.getSiteTable()))) {
            Get get = new Get(Bytes.toBytes(link));
            return table.get(get);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public Result[] getBulk(List<String> links) {
        try (Table table = connection.getTable(TableName.valueOf(config.getSiteTable()))) {
            List<Get> getList = new ArrayList<>();
            for (String link : links) {
                getList.add(new Get(Bytes.toBytes(link)));
            }
            return table.get(getList);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
