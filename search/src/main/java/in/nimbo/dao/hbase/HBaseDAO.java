package in.nimbo.dao.hbase;

import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;

import java.util.List;

public interface HBaseDAO {
    List<Edge> get(String link);

    List<Node> getBulk(String ... links);
}
