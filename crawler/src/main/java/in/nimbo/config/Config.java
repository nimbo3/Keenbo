package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;

import java.io.IOException;
import java.util.Properties;

public class Config {
    private Config() {

    }

    public static Properties loadProperties(String propertyName) {
        try {
            Properties properties = new Properties();
            ClassLoader classLoader = Config.class.getClassLoader();
            properties.load(classLoader.getResourceAsStream(propertyName));
            return properties;
        } catch (IOException e) {
            throw new LoadConfigurationException(propertyName, e);
        }
    }
}
