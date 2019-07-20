package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class HBaseConfig {
    private static final String CONFIG_NAME = "hbase.properties";
    private String linksTable;
    private String referenceCountColumnFamily;
    private String referenceCountColumn;

    public static HBaseConfig load() {
        HBaseConfig config = new HBaseConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setLinksTable(configuration.getString("table.name"));
            config.setReferenceCountColumnFamily(configuration.getString("column.family.name"));
            config.setReferenceCountColumn(configuration.getString("column.name"));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException("hbase.properties", e);
        }
    }

    public void setLinksTable(String linksTable) {
        this.linksTable = linksTable;
    }

    public void setReferenceCountColumnFamily(String referenceCountColumnFamily) {
        this.referenceCountColumnFamily = referenceCountColumnFamily;
    }

    public void setReferenceCountColumn(String referenceCountColumn) {
        this.referenceCountColumn = referenceCountColumn;
    }

    public String getLinksTable() {
        return linksTable;
    }

    public String getReferenceCountColumnFamily() {
        return referenceCountColumnFamily;
    }

    public String getReferenceCountColumn() {
        return referenceCountColumn;
    }
}
