package in.nimbo.dao;

import in.nimbo.config.RedisConfig;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.dao.redis.RedisDAOImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisDAOTest {
    private JedisCluster cluster;
    private RedisDAO redisDAO;
    private Map<String, String> map = new HashMap<>();
    @Before
    public void init() {
        cluster = mock(JedisCluster.class);
        RedisConfig config = new RedisConfig();
        config.setExpireTime(-1);
        config.setHostAndPorts(null);
        redisDAO = new RedisDAOImpl(cluster, config);
    }

    @Test
    public void testContains() {
        when(cluster.get("a")).thenReturn("b");
        boolean contains = redisDAO.contains("a");
        assertTrue(contains);
        when(cluster.get("a")).thenReturn(null);
        contains = redisDAO.contains("a");
        assertFalse(contains);
    }

}
