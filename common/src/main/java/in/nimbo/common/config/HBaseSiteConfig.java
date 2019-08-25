package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSiteConfig {
    private static final String CONFIG_NAME = "hbase-site.properties";
    private String siteTable;
    private byte[] infoColumnFamily;
    private byte[] domainColumnFamily;
    private byte[] rankColumn;
    private byte[] countColumn;
    private byte[] sccColumn;

    public static HBaseSiteConfig load() {
        HBaseSiteConfig config = new HBaseSiteConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setSiteTable(configuration.getString("table"));
            config.setInfoColumnFamily(Bytes.toBytes(configuration.getString("column.family.info")));
            config.setRankColumn(Bytes.toBytes(configuration.getString("column.rank")));
            config.setCountColumn(Bytes.toBytes(configuration.getString("column.count")));
            config.setSccColumn(Bytes.toBytes(configuration.getString("column.scc")));
            config.setDomainColumnFamily(Bytes.toBytes(configuration.getString("column.family.domain")));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getSiteTable() {
        return siteTable;
    }

    public void setSiteTable(String siteTable) {
        this.siteTable = siteTable;
    }

    public byte[] getInfoColumnFamily() {
        return infoColumnFamily;
    }

    public void setInfoColumnFamily(byte[] infoColumnFamily) {
        this.infoColumnFamily = infoColumnFamily;
    }

    public byte[] getDomainColumnFamily() {
        return domainColumnFamily;
    }

    public void setDomainColumnFamily(byte[] domainColumnFamily) {
        this.domainColumnFamily = domainColumnFamily;
    }

    public byte[] getRankColumn() {
        return rankColumn;
    }

    public void setRankColumn(byte[] rankColumn) {
        this.rankColumn = rankColumn;
    }

    public byte[] getCountColumn() {
        return countColumn;
    }

    public void setCountColumn(byte[] countColumn) {
        this.countColumn = countColumn;
    }

    public byte[] getSccColumn() {
        return sccColumn;
    }

    public void setSccColumn(byte[] sccColumn) {
        this.sccColumn = sccColumn;
    }
}
