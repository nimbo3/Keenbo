package in.nimbo.dao.redis;

import in.nimbo.config.RedisConfig;
import redis.clients.jedis.JedisCluster;

import java.util.Date;

public class RedisDAOImpl implements RedisDAO {
    private JedisCluster cluster;
    private RedisConfig config;

    public RedisDAOImpl(JedisCluster cluster, RedisConfig config) {
        this.cluster = cluster;
        this.config = config;
    }

    @Override
    public void add(String link) {
        cluster.set(link, String.valueOf(new Date().getTime()));
        if (config.getExpireTime() > 0){
            cluster.expire(link, config.getExpireTime());
        }
    }

    @Override
    public boolean contains(String link) {
        return cluster.get(link) != null;
    }
}
