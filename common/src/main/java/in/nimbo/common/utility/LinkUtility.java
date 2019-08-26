package in.nimbo.common.utility;

import in.nimbo.common.exception.HashException;
import in.nimbo.common.exception.ReverseLinkException;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class LinkUtility {

    private LinkUtility() {
    }

    /**
     * @param link link
     * @return reversed link (only domain)
     */
    public static String reverseLink(String link) {
        try {
            int length = link.length();
            int i, j;
            i = link.indexOf('/') + 1;
            j = link.indexOf('/', i + 1);
            if (j < 0) {
                j = length;
            }
            String protocol = link.substring(0, i - 1);
            String domain = link.substring(i + 1, j);
            String port = null;
            int colonIndex = domain.indexOf(':');
            if (colonIndex > -1) {
                port = domain.substring(colonIndex);
                domain = domain.substring(0, colonIndex);
            }
            String uri = j < length ? link.substring(j) : "";
            StringBuilder newDomain = new StringBuilder();
            int dotIndex = domain.lastIndexOf('.');
            while (dotIndex > -1) {
                String part = domain.substring(dotIndex + 1);
                newDomain.append(part);
                newDomain.append(".");
                domain = domain.substring(0, dotIndex);
                dotIndex = domain.lastIndexOf('.');
            }
            newDomain.append(domain);
            return protocol + "//" + newDomain + (port != null ? port : "") + uri;
        } catch (StringIndexOutOfBoundsException e) {
            throw new ReverseLinkException(e);
        }
    }

    /**
     * extract domain from a url
     * url must be in absolute format
     *
     * @param link link
     * @return domain of url without it's subdomains
     * @throws MalformedURLException if link is not a illegal url
     */
    public static String getMainDomain(String link) throws MalformedURLException {
        try {
            String domain = getDomain(link);
            int lastDot = domain.lastIndexOf('.');
            int beforeLastDot = domain.substring(0, lastDot).lastIndexOf('.');
            return beforeLastDot == -1 ? domain : domain.substring(beforeLastDot + 1);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
            throw new MalformedURLException("unable to detect domain of url");
        }
    }

    public static String getMainDomainForReversed(String link) throws MalformedURLException {
        try {
            String domain = getDomain(link);
            int firstDot = domain.indexOf('.');
            int afterFirstDot = domain.indexOf('.', firstDot + 1);
            if (afterFirstDot != -1) {
                domain = domain.substring(0, afterFirstDot);
            }
            return domain.substring(firstDot + 1) + "." + domain.substring(0, firstDot);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
            throw new MalformedURLException("unable to detect domain of url");
        }
    }

    private static String getDomain(String link) {
        int indexOfProtocol = link.indexOf('/') + 1;
        int indexOfEndDomain = link.indexOf('/', indexOfProtocol + 1);
        if (indexOfEndDomain < 0) {
            indexOfEndDomain = link.length();
        }
        String domain = link.substring(indexOfProtocol + 1, indexOfEndDomain);
        int colonIndex = domain.indexOf(':');
        if (colonIndex > -1) {
            domain = domain.substring(0, colonIndex);
        }
        return domain;
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
        String protocol = url.getProtocol().toLowerCase();
        String host = url.getHost().toLowerCase();
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

    public static String hashLinkCompressed(String url) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(url.getBytes());
            return new String(digest, StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            throw new HashException(e);
        }
    }

    /**
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
