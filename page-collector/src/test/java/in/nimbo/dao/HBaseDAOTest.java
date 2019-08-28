package in.nimbo.dao;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HBaseDAOTest {
    private static HBaseDAO hBaseDAO;
    private static HBasePageConfig hBasePageConfig;
    private static Connection connection;

    @BeforeClass
    public static void init() throws IOException {
        hBasePageConfig = HBasePageConfig.load();
        connection = ConnectionFactory.createConnection();
        TableName tableName = TableName.valueOf(hBasePageConfig.getPageTable());
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor(hBasePageConfig.getAnchorColumnFamily()));
        descriptor.addFamily(new HColumnDescriptor(hBasePageConfig.getDataColumnFamily()));
        connection.getAdmin().createTable(descriptor);
        hBaseDAO = new HBaseDAOImpl(connection, hBasePageConfig);
    }

    @Before
    public void afterEachTest() throws IOException {
        TableName tableName = TableName.valueOf(hBasePageConfig.getPageTable());
        connection.getAdmin().disableTable(tableName);
        connection.getAdmin().truncateTable(tableName, false);
    }

    @Test
    public void testAdd() throws MalformedURLException {
        Set<Anchor> anchors = new HashSet<>();
        List<Meta> metas = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Anchor anchor = new Anchor("https://google.com/" + i, "content" + i);
            Meta meta = new Meta("key" + i, "content" + i);
            anchors.add(anchor);
            metas.add(meta);
        }
        Page page = new Page("http://www.google.com/", "Google", "content", anchors, metas, 100.0);
        List<Page> pages = new ArrayList<>();
        pages.add(page);
        hBaseDAO.add(pages);
        assertTrue(hBaseDAO.contains(LinkUtility.reverseLink(page.getLink())));
    }

    @Test
    public void testContain() {
        assertFalse(hBaseDAO.contains("http://com.google.www"));
        assertFalse(hBaseDAO.contains("fake link"));
    }
}
