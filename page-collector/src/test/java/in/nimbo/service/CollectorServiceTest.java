package in.nimbo.service;

import in.nimbo.TestUtility;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.*;

public class CollectorServiceTest {
    private static HBaseDAO hBaseDAO;
    private static ElasticDAO elasticDAO;
    private static CollectorService collectorService;
    private static List<Page> pages;
    private static List<Page> pagesWithEmptyAnchor;

    @BeforeClass
    public static void init() throws MalformedURLException {
        TestUtility.setMetricRegistry();
        Set<Anchor> anchors = new HashSet<>();
        pages = new ArrayList<>();
        pagesWithEmptyAnchor = new ArrayList<>();
        anchors.add(new Anchor("http://google.com", "google"));
        anchors.add(new Anchor("http://sahab.ir", "sahab"));
        Page page = new Page("http://nimbo.in", "nimbo", "sahab internship", anchors, new ArrayList<>(), 1.0);
        pages.add(page);
        Page pageWithEmptyAnchor = new Page("http://nimbo.in", "nimbo", "sahab internship", new HashSet<>(), new ArrayList<>(), 1.0);
        pagesWithEmptyAnchor.add(pageWithEmptyAnchor);
    }

    @Before
    public void beforeEachTest() {
        hBaseDAO = mock(HBaseDAO.class);
        elasticDAO = mock(ElasticDAO.class);
        collectorService = new CollectorService(hBaseDAO, elasticDAO, true);
    }

    @Test
    public void handleTest() {
        doNothing().when(hBaseDAO).add(pages, true);
        doNothing().when(hBaseDAO).add(pages, false);
        for (Page page : pages) {
            doNothing().when(elasticDAO).save(page);
        }
        Assert.assertTrue(collectorService.processList(pages));
    }

    @Test
    public void handleWithEmptyAnchorTest() {
        doNothing().when(hBaseDAO).add(pagesWithEmptyAnchor, true);
        doNothing().when(hBaseDAO).add(pagesWithEmptyAnchor, false);
        for (Page page : pagesWithEmptyAnchor) {
            doNothing().when(elasticDAO).save(page);
        }
        Assert.assertTrue(collectorService.processList(pagesWithEmptyAnchor));
    }

    @Test
    public void handleWithoutSaveToHBase() {
        doThrow(HBaseException.class).when(hBaseDAO).add(pages, true);
        doThrow(HBaseException.class).when(hBaseDAO).add(pages, false);
        for (Page page : pages) {
            doNothing().when(elasticDAO).save(page);
        }
        Assert.assertFalse(collectorService.processList(pages));
    }

    @Test
    public void handleWithElasticException() {
        doNothing().when(hBaseDAO).add(pages, true);
        doNothing().when(hBaseDAO).add(pages, false);
        for (Page page : pages) {
            doThrow(ElasticException.class).when(elasticDAO).save(page);
        }
        Assert.assertFalse(collectorService.processList(pages));
    }
}
