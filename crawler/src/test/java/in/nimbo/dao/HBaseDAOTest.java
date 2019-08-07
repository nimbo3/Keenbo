package in.nimbo.dao;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class HBaseDAOTest {
    private static HBaseDAO hBaseDAO;
    private static HBaseConfig hBaseConfig;
    private static Connection connection;

    @BeforeClass
    public static void init() {
        hBaseConfig = HBaseConfig.load();
        try {
            connection = ConnectionFactory.createConnection();
            TableName tableName = TableName.valueOf(hBaseConfig.getLinksTable());
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor(hBaseConfig.getAnchorColumnFamily()));
            descriptor.addFamily(new HColumnDescriptor(hBaseConfig.getMetaColumnFamily()));
            connection.getAdmin().createTable(descriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hBaseDAO = new HBaseDAOImpl(connection, hBaseConfig);
    }

    @Test
    public void testAdd() throws IOException {
        Set<Anchor> anchors = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Anchor anchor = new Anchor("https://google.com/" + i, "content" + i);
            anchors.add(anchor);
        }
        List<Meta> metas = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Meta meta = new Meta("key" + i, "content" + i);
            metas.add(meta);
        }
        Page page = new Page("http://www.google.com/", "Google", "a", anchors, metas, 100.0);
        hBaseDAO.add(page);
        assertTrue(hBaseDAO.contains("http://com.google.www/"));
    }
}
