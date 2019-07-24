package in.nimbo;

import in.nimbo.utility.LinkUtility;
import org.junit.Test;

import java.net.MalformedURLException;

import static org.junit.Assert.assertEquals;

public class LinkUtilityTest {
    @Test
    public void testReverseLink() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com/uri?query=1&string=2";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www/uri?query=1&string=2");
    }

    @Test
    public void testWithoutQuery() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com/uri";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www/uri");
    }

    @Test
    public void testWithoutURI() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www");
    }

    @Test
    public void testWithPort() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com:8080/uri";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www:8080/uri");
    }

    @Test(expected = MalformedURLException.class)
    public void testWithoutProtocol() throws MalformedURLException {
        LinkUtility.reverseLink("www.google.com");
    }
}
