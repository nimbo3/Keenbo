package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePageConfig {
    private static final String CONFIG_NAME = "hbase-page.properties";
    private String pageTable;
    private byte[] anchorColumnFamily;
    private byte[] metaColumnFamily;
    private byte[] rankColumnFamily;
    private byte[] rankColumn;

    public static HBasePageConfig load() {
        HBasePageConfig config = new HBasePageConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setPageTable(configuration.getString("table"));
            config.setAnchorColumnFamily(Bytes.toBytes(configuration.getString("column.family.anchor")));
            config.setMetaColumnFamily(Bytes.toBytes(configuration.getString("column.family.meta")));
            config.setRankColumnFamily(Bytes.toBytes(configuration.getString("column.family.rank")));
            config.setRankColumn(Bytes.toBytes(configuration.getString("column.rank")));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public byte[] getRankColumnFamily() {
        return rankColumnFamily;
    }

    public void setRankColumnFamily(byte[] rankColumnFamily) {
        this.rankColumnFamily = rankColumnFamily;
    }

    public byte[] getRankColumn() {
        return rankColumn;
    }

    public void setRankColumn(byte[] rankColumn) {
        this.rankColumn = rankColumn;
    }

    public String getPageTable() {
        return pageTable;
    }

    public void setPageTable(String pageTable) {
        this.pageTable = pageTable;
    }

    public byte[] getAnchorColumnFamily() {
        return anchorColumnFamily;
    }

    public void setAnchorColumnFamily(byte[] anchorColumnFamily) {
        this.anchorColumnFamily = anchorColumnFamily;
    }

    public byte[] getMetaColumnFamily() {
        return metaColumnFamily;
    }

    public void setMetaColumnFamily(byte[] metaColumnFamily) {
        this.metaColumnFamily = metaColumnFamily;
    }
}
