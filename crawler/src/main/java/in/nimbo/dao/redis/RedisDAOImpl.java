package in.nimbo.dao.redis;

import in.nimbo.config.RedisConfig;
import in.nimbo.entity.RedisNodeStatus;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

    @Override
    public List<RedisNodeStatus> memoryUsage() {
        List<RedisNodeStatus> statuses = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();
        for (String key : clusterNodes.keySet()) {
            JedisPool jedisPool = clusterNodes.get(key);
            String memStatus = jedisPool.getResource().info("Memory");
            String[] memStatusSplit = memStatus.trim().split("\n");
            for (String memStatusPart : memStatusSplit) {
                String[] memStatusPartSplit = memStatusPart.trim().split(":");
                if (memStatusPartSplit.length == 2) {
                    String memStatusPartKey = memStatusPartSplit[0].trim();
                    String memStatusPartValue = memStatusPartSplit[1].trim();
                    if (memStatusPartKey.equals("used_memory")) {
                        long value = Long.valueOf(memStatusPartValue);
                        statuses.add(new RedisNodeStatus(key, value));
                        break;
                    }
                }
            }
        }
        return statuses;
    }
}
