package in.nimbo.common.utility;

import in.nimbo.common.exception.HashException;
import in.nimbo.common.exception.LinkUtilityException;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;

public class LinkUtility {

    private LinkUtility() {
    }

    /**
     * @param link link
     * @return reversed link (only domain)
     */
    public static String reverseLink(String link) {
        try {
            URL url = new URL(link);
            String host = url.getHost();
            String[] hostParts = host.split("\\.");
            Collections.reverse(Arrays.asList(hostParts));
            String newHost = String.join(".", hostParts);
            String protocol = url.getProtocol();
            int port = url.getPort();
            String uri = url.getPath();
            String query = url.getQuery();
            String answer = protocol + "://" + newHost + (port != -1 ? ":" + port : "");
            if (uri != null) {
                answer += uri;
            }
            if (query != null) {
                answer += "?";
                answer += query;
            }
            return answer;
        } catch (MalformedURLException e) {
            throw new LinkUtilityException(e);
        }
    }

    /**
     * extract domain from a url
     * url must be in absolute format
     *
     * @param link link
     * @return domain of url without it's subdomains
     * @throws URISyntaxException if link is not a illegal url
     */
    public static String getMainDomain(String link) throws URISyntaxException {
        try {
            URI uri = new URI(link);
            String host = uri.getHost();
            String[] hostParts = host.split("\\.");
            return hostParts[hostParts.length - 2] + "." + hostParts[hostParts.length - 1];
        } catch (IndexOutOfBoundsException | NullPointerException e) {
            throw new URISyntaxException(link, "unable to detect host of url");
        }
    }

    /**
     * check whether a url is a valid url or not
     *
     * @param link url
     * @return true if url is a valid url
     */
    public static boolean isValidUrl(String link) {
        try {
            URI uri = new URL(link).toURI();
            return uri.getHost() != null && uri.getHost().split("\\.").length >= 2;
        } catch (MalformedURLException | URISyntaxException | NullPointerException e) {
            return false;
        }
    }

    public static String normalize(String link) throws MalformedURLException {
        URL url = new URL(link);
        String protocol = url.getProtocol();
        String host = url.getHost();
        int port = url.getPort();
        String uri = url.getPath();
        String newLink = protocol + "://" + host;
        if (port != -1) {
            newLink += ":" + port;
        }
        if (uri != null) {
            if (uri.endsWith("/")) {
                newLink += uri.substring(0, uri.length() - 1);
            } else {
                newLink += uri;
            }
        }
        return newLink;
    }

    /**
     * has a string with md5 hash
     *
     * @param url url
     * @return hash of url
     */
    public static String hashLink(String url) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(url.getBytes());
            BigInteger number = new BigInteger(1, digest);
            StringBuilder hashText = new StringBuilder(number.toString(16));
            while (hashText.length() < 32) {
                hashText.insert(0, "0");
            }
            return hashText.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new HashException(e);
        }
    }

    /**
     *
     * @param link link which must be normalized
     * @return depth of a uri in url
     * @throws MalformedURLException if link is illegal
     */
    public static long depth(String link) throws MalformedURLException {
        URL url = new URL(link);
        String uri = url.getPath();
        return uri.isEmpty() || uri.equals("/") ? 0 : uri.split("/").length - 1;
    }
}
