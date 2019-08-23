package in.nimbo.redis;

import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisDAOImpl implements RedisDAO {
    private RedissonClient redissonClient;

    public RedisDAOImpl(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public List<?> contains(List<String> links) {
        BatchOptions options = BatchOptions.defaults()
                .executionMode(BatchOptions.ExecutionMode.IN_MEMORY)
                .retryInterval(2, TimeUnit.SECONDS)
                .retryAttempts(4);

        RBatch batch = redissonClient.createBatch(options);
        for (String link : links) {
            batch.getBucket(link).isExistsAsync();
        }
        return batch.execute().getResponses();
    }

}
