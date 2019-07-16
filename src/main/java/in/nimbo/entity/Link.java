package in.nimbo.entity;

public class Link {
    private String protocol;
    private String host;
    private int port;
    private String uri;
    private String domain;

    public String getDomain() {
        return domain;
    }

    public Link(String protocol, String host, int port, String uri) {
        this.protocol = protocol;
        this.host = host;
        this.port = port != -1 ? port : protocol.equals("http") ? 80 : 443;
        this.uri = uri;
        String[] strs = host.split("\\.");
        this.domain = strs[strs.length - 2] + "." + strs[strs.length - 1];

    }

    public String getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUri() {
        return uri;
    }
}
