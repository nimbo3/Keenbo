package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConfig {
    private static final String CONFIG_NAME = "hbase.properties";
    private String pageTable;
    private String siteTable;
    private byte[] anchorColumnFamily;
    private byte[] dataColumnFamily;
    private byte[] pageRankColumn;
    private byte[] infoColumnFamily;
    private byte[] domainColumnFamily;
    private byte[] siteRankColumn;
    private byte[] countColumn;

    public static HBaseConfig load() {
        HBaseConfig config = new HBaseConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setPageTable(configuration.getString("page.table"));
            config.setAnchorColumnFamily(Bytes.toBytes(configuration.getString("page.column.family.anchor")));
            config.setDataColumnFamily(Bytes.toBytes(configuration.getString("page.column.family.data")));
            config.setPageRankColumn(Bytes.toBytes(configuration.getString("page.column.rank")));
            config.setSiteTable(configuration.getString("site.table"));
            config.setInfoColumnFamily(Bytes.toBytes(configuration.getString("site.column.family.info")));
            config.setSiteRankColumn(Bytes.toBytes(configuration.getString("site.column.rank")));
            config.setCountColumn(Bytes.toBytes(configuration.getString("site.column.count")));
            config.setDomainColumnFamily(Bytes.toBytes(configuration.getString("site.column.family.domain")));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getPageTable() {
        return pageTable;
    }

    public void setPageTable(String pageTable) {
        this.pageTable = pageTable;
    }

    public String getSiteTable() {
        return siteTable;
    }

    public void setSiteTable(String siteTable) {
        this.siteTable = siteTable;
    }

    public byte[] getAnchorColumnFamily() {
        return anchorColumnFamily;
    }

    public void setAnchorColumnFamily(byte[] anchorColumnFamily) {
        this.anchorColumnFamily = anchorColumnFamily;
    }

    public byte[] getDataColumnFamily() {
        return dataColumnFamily;
    }

    public void setDataColumnFamily(byte[] dataColumnFamily) {
        this.dataColumnFamily = dataColumnFamily;
    }

    public byte[] getPageRankColumn() {
        return pageRankColumn;
    }

    public void setPageRankColumn(byte[] pageRankColumn) {
        this.pageRankColumn = pageRankColumn;
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

    public byte[] getSiteRankColumn() {
        return siteRankColumn;
    }

    public void setSiteRankColumn(byte[] siteRankColumn) {
        this.siteRankColumn = siteRankColumn;
    }

    public byte[] getCountColumn() {
        return countColumn;
    }

    public void setCountColumn(byte[] countColumn) {
        this.countColumn = countColumn;
    }
}
