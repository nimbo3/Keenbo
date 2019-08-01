package in.nimbo.dao;

import in.nimbo.config.RedisConfig;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.dao.redis.RedisDAOImpl;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisCluster;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class RedisDAOTest {
    private RedisConfig redisConfig;
    private JedisCluster cluster;
    private RedisDAO redisDAO;

    @Before
    public void init() {
        cluster = mock(JedisCluster.class);
        redisConfig = new RedisConfig();
        redisConfig.setExpireTime(-1);
        redisConfig.setHostAndPorts(null);
        redisDAO = new RedisDAOImpl(cluster, redisConfig);
    }

    @Test
    public void testContains() {
        when(cluster.get("key")).thenReturn("key");
        boolean contains = redisDAO.contains("key");
        assertTrue(contains);
        when(cluster.get("key")).thenReturn(null);
        contains = redisDAO.contains("key");
        assertFalse(contains);
    }

    @Test
    public void testAdd() {
        doReturn("key").when(cluster).set(anyString(), anyString());
        doReturn(1L).when(cluster).expire(anyString(), anyInt());
        redisConfig.setExpireTime(1);
        redisDAO.add("key");
    }
}
