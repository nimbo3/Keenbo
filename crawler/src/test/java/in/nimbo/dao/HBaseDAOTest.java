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
            descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("anchor")));
            descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("meta")));
            connection.getAdmin().createTable(descriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hBaseDAO = new HBaseDAOImpl(connection, hBaseConfig);
    }

    @Test
    public void testAdd() throws IOException {
        Page page = new Page();
        page.setReversedLink("http://com.google.www/");
        page.setLink("http://www.google.com/");
        page.setTitle("Google");
        page.setContent("a");
        page.setRank(100.0);
        Set<Anchor> anchors = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Anchor anchor = new Anchor("https://google.com/" + i, "content" + i);
            anchors.add(anchor);
        }
        page.setAnchors(anchors);
        List<Meta> metas = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Meta meta = new Meta("key" + i, "content" + i);
            metas.add(meta);
        }
        page.setMetas(metas);
        assertTrue(hBaseDAO.add(page));
    }

    /*@Test
    public void testContains() throws IOException {
        assertFalse(hBaseDAO.contains("link"));
    }

    @Test
    public void testAddWithException() throws IOException {
        Page page = new Page();
        page.setReversedLink("http://com.google.www/");
        page.setContent("a");
        Set<Anchor> anchors = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Anchor anchor = new Anchor("https://google.com/" + i, "content" + i);
            anchors.add(anchor);
        }
        page.setAnchors(anchors);
        List<Meta> metas = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Meta meta = new Meta("key" + i, "content" + i);
            metas.add(meta);
        }
        page.setMetas(metas);
        hBaseDAO.add(page);
    }

    @Test
    public void testAddWithIllegalArgumentException() throws IOException {
        Page page = new Page();
        page.setReversedLink("http://com.google.www/");
        page.setContent("a");
        Set<Anchor> anchors = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Anchor anchor = new Anchor("https://google.com/" + i, "content" + i);
            anchors.add(anchor);
        }
        page.setAnchors(anchors);
        List<Meta> metas = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Meta meta = new Meta("key" + i, "content" + i);
            metas.add(meta);
        }
        page.setMetas(metas);
        assertTrue(hBaseDAO.add(page));
    }

    @Test
    public void testClose() throws IOException {
        hBaseDAO.close();
    }*/
}
