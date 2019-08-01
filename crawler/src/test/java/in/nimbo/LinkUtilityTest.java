package in.nimbo;

import in.nimbo.utility.LinkUtility;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class LinkUtilityTest {
    @Test
    public void testReverseLink() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com/uri?query=1&string=2";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals("http://com.stackoverflow.blog.www/uri?query=1&string=2", reverseLink);
    }

    @Test
    public void testReverseLinkWithoutQuery() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com/uri";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals("http://com.stackoverflow.blog.www/uri", reverseLink);
    }

    @Test
    public void testReverseLinkWithoutURI() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals("http://com.stackoverflow.blog.www", reverseLink);
    }

    @Test
    public void testReverseLinkWithPort() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com:8080/uri";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals("http://com.stackoverflow.blog.www:8080/uri", reverseLink);
    }

    @Test(expected = MalformedURLException.class)
    public void testReverseLinkWithoutProtocol() throws MalformedURLException {
        LinkUtility.reverseLink("www.google.com");
    }

    @Test(expected = URISyntaxException.class)
    public void testGetMainDomainInvalidUrl() throws URISyntaxException {
        LinkUtility.getMainDomain("invalid");
    }

    @Test(expected = URISyntaxException.class)
    public void testGetMainDomainNullUrl() throws URISyntaxException {
        LinkUtility.getMainDomain("https://salam");
    }

    @Test
    public void testIsValidUrl() throws URISyntaxException {
        assertFalse(LinkUtility.isValidUrl(null));
        assertFalse(LinkUtility.isValidUrl("invalid"));
        assertFalse(LinkUtility.isValidUrl("https://salam"));
        assertTrue(LinkUtility.isValidUrl("https://nimbo.in"));
    }

    @Test
    public void testNormalize() throws MalformedURLException {
        String site = "https://stackoverflow.blog?blb=1";
        String normalize = LinkUtility.normalize(site);
        assertEquals("https://stackoverflow.blog", normalize);
    }

    @Test
    public void testNormalizeWithPort() throws MalformedURLException {
        String site = "https://stackexchange.com:9200/sites#culturerecreation";
        String normalize = LinkUtility.normalize(site);
        assertEquals("https://stackexchange.com:9200/sites", normalize);
    }

    @Test
    public void testNormalizeEndPart() throws MalformedURLException {
        String site = "https://stackexchange.com:9200/sites/";
        String normalize = LinkUtility.normalize(site);
        assertEquals("https://stackexchange.com:9200/sites", normalize);
    }

    @Test
    public void testNormalizeWithoutURI() throws MalformedURLException {
        String site = "https://chat.stackexchange.com?tab=site&host=askubuntu.com";
        String normalize = LinkUtility.normalize(site);
        assertEquals("https://chat.stackexchange.com", normalize);
    }

    @Test
    public void testNormalizeWithoutURIWithFragment() throws MalformedURLException {
        String site = "https://askubuntu.com#";
        String normalize = LinkUtility.normalize(site);
        assertEquals("https://askubuntu.com", normalize);
    }

    @Test
    public void testNormalizeWithStrangeURL() throws MalformedURLException {
        String site = "https://launchpad.net/+login";
        String normalize = LinkUtility.normalize(site);
        assertEquals("https://launchpad.net/+login", normalize);
    }


}
