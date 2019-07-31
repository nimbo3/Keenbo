package in.nimbo.dao.redis;

import in.nimbo.entity.RedisNodeStatus;

import java.util.List;

public interface RedisDAO {
    void add(String link);

    boolean contains(String link);

    List<RedisNodeStatus> memoryUsage();
}
