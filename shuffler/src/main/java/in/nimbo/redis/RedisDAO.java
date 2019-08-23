package in.nimbo.redis;

import java.util.List;

public interface RedisDAO {
    List<?> contains(List<String> links);
}
