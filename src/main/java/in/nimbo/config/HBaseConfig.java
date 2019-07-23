package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class HBaseConfig {
    private static final String CONFIG_NAME = "hbase.properties";
    private String linksTable;
    private String contentColumnFamily;
    private String contentColumn;
    private String anchorsColumnFamily;
    private String metasColumnFamily;

    public static HBaseConfig load() {
        HBaseConfig config = new HBaseConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setLinksTable(configuration.getString("table.name"));
            config.setContentColumnFamily(configuration.getString("column.family.content.name"));
            config.setAnchorsColumnFamily(configuration.getString("column.family.anchors.name"));
            config.setContentColumn(configuration.getString("column.content.name"));
            config.setMetasColumnFamily(configuration.getString("column.family.metas.name"));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getMetasColumnFamily() {
        return metasColumnFamily;
    }

    public void setMetasColumnFamily(String metasColumnFamily) {
        this.metasColumnFamily = metasColumnFamily;
    }

    public void setAnchorsColumnFamily(String anchorsColumnFamily) {
        this.anchorsColumnFamily = anchorsColumnFamily;
    }

    public String getAnchorsColumnFamily() {
        return anchorsColumnFamily;
    }

    public void setLinksTable(String linksTable) {
        this.linksTable = linksTable;
    }

    public void setContentColumnFamily(String contentColumnFamily) {
        this.contentColumnFamily = contentColumnFamily;
    }

    public void setContentColumn(String contentColumn) {
        this.contentColumn = contentColumn;
    }

    public String getLinksTable() {
        return linksTable;
    }

    public String getContentColumnFamily() {
        return contentColumnFamily;
    }

    public String getContentColumn() {
        return contentColumn;
    }
}
