package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RedisConfig {
    private static final String CONFIG_NAME = "redis.properties";
    private int expireTime;
    private Set<HostAndPort> hostAndPorts;

    public static RedisConfig load(){
        RedisConfig redisConfig = new RedisConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            redisConfig.setExpireTime(config.getInt("redis.expire"));
            String hostsConfigFile = config.getString("redis.hosts");
            PropertiesConfiguration hostsConfig = new PropertiesConfiguration(hostsConfigFile);
            Iterator<String> hostsIterator = hostsConfig.getKeys();
            Set<HostAndPort> hostAndPorts = new HashSet<>();
            while (hostsIterator.hasNext()){
                String key = hostsIterator.next();
                HostAndPort hostAndPort = new HostAndPort(key, hostsConfig.getInt(key));
                hostAndPorts.add(hostAndPort);
            }
            redisConfig.setHostAndPorts(hostAndPorts);
            return redisConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public Set<HostAndPort> getHostAndPorts() {
        return hostAndPorts;
    }

    public void setHostAndPorts(Set<HostAndPort> hostAndPorts) {
        this.hostAndPorts = hostAndPorts;
    }

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }
}
