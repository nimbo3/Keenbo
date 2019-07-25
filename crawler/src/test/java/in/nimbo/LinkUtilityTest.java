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
    public void testReverseLinkWithoutQuery() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com/uri";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www/uri");
    }

    @Test
    public void testReverseLinkWithoutURI() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www");
    }

    @Test
    public void testReverseLinkWithPort() throws MalformedURLException {
        String site = "http://www.blog.stackoverflow.com:8080/uri";
        String reverseLink = LinkUtility.reverseLink(site);
        assertEquals(reverseLink, "http://com.stackoverflow.blog.www:8080/uri");
    }

    @Test(expected = MalformedURLException.class)
    public void testReverseLinkWithoutProtocol() throws MalformedURLException {
        LinkUtility.reverseLink("www.google.com");
    }

    @Test
    public void testNormalize() {
        String site = "https://stackoverflow.blog?blb=1";
        String normalize = LinkUtility.normalize(site);
        assertEquals(normalize, "https://stackoverflow.blog");
    }

    @Test
    public void testNormalizeWithPort(){
        String site = "https://stackexchange.com/sites#culturerecreation";
        String normalize = LinkUtility.normalize(site);
        assertEquals(normalize, "https://stackexchange.com/sites");
    }

    @Test
    public void testNormalizeWithoutURI() {
        String site = "https://chat.stackexchange.com?tab=site&host=askubuntu.com";
        String normalize = LinkUtility.normalize(site);
        assertEquals(normalize, "https://chat.stackexchange.com");
    }

    @Test
    public void testNormalizeWithoutURIWithFragment() {
        String site = "https://askubuntu.com#";
        String normalize = LinkUtility.normalize(site);
        assertEquals(normalize, "https://askubuntu.com");
    }

    @Test
    public void testNormalizeWithStrangeURL() {
        String site = "https://launchpad.net/+login";
        String normalize = LinkUtility.normalize(site);
        assertEquals(normalize, "https://launchpad.net/+login");
    }
}
