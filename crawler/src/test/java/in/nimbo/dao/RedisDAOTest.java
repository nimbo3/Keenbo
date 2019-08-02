package in.nimbo.dao;

import in.nimbo.common.config.RedisConfig;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.dao.redis.RedisDAOImpl;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisCluster;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class RedisDAOTest {
    private RedisConfig redisConfig;
    private JedisCluster cluster;
    private RedisDAO redisDAO;

    @Before
    public void init() {
        cluster = mock(JedisCluster.class);
        redisConfig = RedisConfig.load();
        redisDAO = new RedisDAOImpl(cluster, redisConfig);
    }

    @Test
    public void testContains() {
        redisConfig.setExpireTime(-1);
        assertEquals(3, redisConfig.getHostAndPorts().size());
        when(cluster.get("key")).thenReturn("key");
        boolean contains = redisDAO.contains("key");
        assertTrue(contains);
        when(cluster.get("key")).thenReturn(null);
        contains = redisDAO.contains("key");
        assertFalse(contains);
    }

    @Test
    public void testAdd() {
        try {
            doReturn("key").when(cluster).set(anyString(), anyString());
            doReturn(1L).when(cluster).expire(anyString(), anyInt());
            redisConfig.setExpireTime(1);
            redisDAO.add("key");
        } catch (Exception e) {
            fail();
        }
    }
}
