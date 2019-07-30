package in.nimbo.dao;

import in.nimbo.config.HBaseConfig;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class HBaseDAOTest {
    private static HBaseDAO hBaseDAO;
    private static HBaseConfig hBaseConfig;
    private static Connection connection;

    @BeforeClass
    public static void init() {
        hBaseConfig = HBaseConfig.load();
        connection = mock(Connection.class);
        hBaseDAO = new HBaseDAOImpl(connection, hBaseConfig);
    }

    @Test
    public void testAdd() throws IOException {
        Table table = mock(Table.class);
        when(connection.getTable(any(TableName.class))).thenReturn(table);
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
        doNothing().when(table).put(any(Put.class));
        assertTrue(hBaseDAO.add(page));
    }

    @Test(expected = HBaseException.class)
    public void testAddWithException() throws IOException {
        Table table = mock(Table.class);
        when(connection.getTable(any(TableName.class))).thenThrow(IOException.class);
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
        doNothing().when(table).put(any(Put.class));
        hBaseDAO.add(page);
    }

    @Test
    public void testAddWithIllegalArgumentException() throws IOException {
        Table table = mock(Table.class);
        when(connection.getTable(any(TableName.class))).thenReturn(table);
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
        doThrow(IllegalArgumentException.class).when(table).put(any(Put.class));
        assertFalse(hBaseDAO.add(page));
    }

    @Test
    public void testClose() throws IOException {
        try {
            doNothing().when(connection).close();
            hBaseDAO.close();
        } catch (IOException e) {
            fail();
        }
    }
}
