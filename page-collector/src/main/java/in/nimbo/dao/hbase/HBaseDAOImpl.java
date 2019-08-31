package in.nimbo.dao.hbase;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.service.keyword.KeywordExtractorService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseDAOImpl implements HBaseDAO {
    private Logger logger = LoggerFactory.getLogger("collector");
    private HBasePageConfig config;
    private Connection connection;

    public HBaseDAOImpl(Connection connection, HBasePageConfig config) {
        this.connection = connection;
        this.config = config;
    }

    public void close() throws IOException {
        connection.close();
    }

    @Override
    public void add(List<Page> pages, boolean extractKeyword) {
        List<Put> puts = new ArrayList<>();
        for (Page page : pages) {
            puts.add(getPut(page, extractKeyword));
        }

        if (connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection();
            } catch (IOException e) {
                throw new HBaseException(e);
            }
        }

        try (Table table = connection.getTable(TableName.valueOf(config.getPageTable()))) {
            logger.info("Start sending bulk put to HBase");
            table.put(puts);
            logger.info("Finish sending bulk put to HBase");
        } catch (IllegalArgumentException | IOException e) {
            throw new HBaseException(e);
        }
    }

    @Override
    public boolean contains(String link) {
        try (Table table = connection.getTable(TableName.valueOf(config.getPageTable()))) {
            Get get = new Get(Bytes.toBytes(link));
            return table.exists(get);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }

    private Put getPut(Page page, boolean extractKeyword) {
        Put put = new Put(Bytes.toBytes(page.getReversedLink()));

        for (Anchor anchor : page.getAnchors()) {
            put.addColumn(config.getAnchorColumnFamily(),
                    Bytes.toBytes(anchor.getHref()), Bytes.toBytes(anchor.getContent()));
        }

        if (extractKeyword) {
            Map<String, Integer> keywords = KeywordExtractorService.extractKeywords(page.getContent());
            for (Map.Entry<String, Integer> keyword : keywords.entrySet()) {
                put.addColumn(config.getDataColumnFamily(),
                        Bytes.toBytes(keyword.getKey()), Bytes.toBytes(Integer.toString(keyword.getValue())));
            }
        }

        put.addColumn(config.getDataColumnFamily(), config.getRankColumn(), Bytes.toBytes("1"));
        return put;
    }
}
