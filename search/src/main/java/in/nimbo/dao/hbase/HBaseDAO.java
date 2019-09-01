package in.nimbo.dao.hbase;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public interface HBaseDAO {
    Result get(String link);

    Result[] getBulk(List<String> links);
}
