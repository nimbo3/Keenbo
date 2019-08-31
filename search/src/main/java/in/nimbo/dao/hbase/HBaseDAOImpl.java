package in.nimbo.dao.hbase;

import in.nimbo.common.config.HBasePageConfig;
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

    @Override
    public List<Edge> get(String link) {
        try (Table table = connection.getTable(TableName.valueOf(config.getSiteTable()))) {
            Get get = new Get(Bytes.toBytes(link));
            Result result = table.get(get);
            return result.listCells().stream().filter(cell -> CellUtil.matchingFamily(cell, config.getDomainColumnFamily()))
                    .map(cell -> new Edge(link, Bytes.toString(CellUtil.cloneQualifier(cell)), Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)))))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public List<Node> getBulk(String... links) {
        try (Table table = connection.getTable(TableName.valueOf(config.getSiteTable()))) {
            List<Get> getList = new ArrayList<>();
            for (String link : links) {
                getList.add(new Get(Bytes.toBytes(link)));
            }
            Result[] results = table.get(getList);
            List<Node> nodes = new ArrayList<>();
            for (Result result : results) {
                double rank = Double.valueOf(Bytes.toString(result.getValue(config.getInfoColumnFamily(), config.getRankColumn())));
                int count = Integer.valueOf(Bytes.toString(result.getValue(config.getInfoColumnFamily(), config.getCountColumn())));
                String rowKey = Bytes.toString(result.getRow());
                Node node = new Node();
                node.setId(rowKey);
                node.setFont(new Font(rank));
                node.setPageCount(count);
                nodes.add(node);
            }
            return nodes;
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
