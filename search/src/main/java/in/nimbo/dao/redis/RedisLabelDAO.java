package in.nimbo.dao.redis;

import in.nimbo.config.SparkConfig;
import redis.clients.jedis.Jedis;

public class RedisLabelDAO implements LabelDAO {
    private Jedis jedis;
    private SparkConfig sparkConfig;

    public RedisLabelDAO(Jedis jedis, SparkConfig sparkConfig) {
        this.jedis = jedis;
        this.sparkConfig = sparkConfig;
    }

    @Override
    public void add(String url, int label) {
        jedis.set(url, String.valueOf(label));
        jedis.expire(url, sparkConfig.getCacheExpire());
    }

    @Override
    public Integer get(String url) {
        String labelString = jedis.get(url);
        if (labelString != null) {
            return Integer.valueOf(labelString);
        }
        return null;
    }
}
