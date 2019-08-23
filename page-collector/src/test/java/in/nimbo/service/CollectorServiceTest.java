package in.nimbo.service;

import in.nimbo.TestUtility;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import org.elasticsearch.ElasticsearchException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

public class CollectorServiceTest {
    private static HBaseDAO hBaseDAO;
    private static ElasticDAO elasticDAO;
    private static CollectorService collectorService;
    private static Page page;
    private static Page pageWithEmptyAnchor;

    @BeforeClass
    public static void init() throws MalformedURLException {
        TestUtility.setMetricRegistry();
        Set<Anchor> anchors = new HashSet<>();
        anchors.add(new Anchor("http://google.com", "google"));
        anchors.add(new Anchor("http://sahab.ir", "sahab"));
        page = new Page("http://nimbo.in", "nimbo", "sahab internship", anchors, new ArrayList<>(), 1.0);
        pageWithEmptyAnchor = new Page("http://nimbo.in", "nimbo", "sahab internship", new HashSet<>(), new ArrayList<>(), 1.0);
    }

    @Before
    public void beforeEachTest() {
        hBaseDAO = mock(HBaseDAO.class);
        elasticDAO = mock(ElasticDAO.class);
        collectorService = new CollectorService(hBaseDAO, elasticDAO);
    }

    @Test
    public void handleTest() {
        when(hBaseDAO.add(page)).thenReturn(true);
        doNothing().when(elasticDAO).save(page);
        Assert.assertTrue(collectorService.handle(page));
    }

    @Test
    public void handleWithEmptyAnchorTest() {
        when(hBaseDAO.add(pageWithEmptyAnchor)).thenReturn(true);
        doNothing().when(elasticDAO).save(pageWithEmptyAnchor);
        Assert.assertTrue(collectorService.handle(pageWithEmptyAnchor));
    }

    @Test
    public void handleWithoutSaveToHBase() {
        when(hBaseDAO.add(page)).thenReturn(false);
        doNothing().when(elasticDAO).save(page);
        Assert.assertFalse(collectorService.handle(page));
    }

    @Test
    public void handleWithHBaseException() {
        when(hBaseDAO.add(page)).thenThrow(HBaseException.class);
        doNothing().when(elasticDAO).save(page);
        Assert.assertFalse(collectorService.handle(page));
    }

    @Test
    public void handleWithElasticException() {
        when(hBaseDAO.add(page)).thenReturn(true);
        doThrow(ElasticsearchException.class).when(elasticDAO).save(page);
        Assert.assertFalse(collectorService.handle(page));
    }
}
