package serializer;

import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import in.nimbo.common.serializer.PageDeserializer;
import in.nimbo.common.serializer.PageSerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashSet;

public class PageSerializerTest {

    private static Page page;
    private static PageSerializer pageSerializer;
    private static PageDeserializer pageDeserializer;

    @BeforeClass
    public static void init() throws MalformedURLException {
        page = new Page(
                "http://nimbo.in/mentores",
                "mentors | nimbo",
                "Be your best",
                new HashSet<>(Collections.singletonList(new Anchor("http://google.com", "search in google"))),
                Collections.singletonList(new Meta("key", "content")),
                5.8);
        pageSerializer = new PageSerializer();
        pageDeserializer = new PageDeserializer();
    }

    @Test
    public void testSerializer() {
        byte[] json = pageSerializer.serialize("topic", page);
        Page page1 = pageDeserializer.deserialize("some topic", json);
        Assert.assertEquals(page.getLink(), page1.getLink());
        Assert.assertEquals(page.getTitle(), page1.getTitle());
        Assert.assertEquals(page.getContent(), page1.getContent());
        Assert.assertEquals(page.getMetas(), page1.getMetas());
        Assert.assertEquals(page.getAnchors(), page1.getAnchors());
        Assert.assertEquals(page.getRank(), page1.getRank(), 0.0);
    }
}
