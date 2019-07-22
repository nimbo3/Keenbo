package in.nimbo.utility;

import in.nimbo.exception.LoadConfigurationException;

import java.io.IOException;
import java.util.Properties;

public class Utility {
    private Utility() {
    }

    public static Properties loadProperties(String propertyName) {
        try {
            Properties properties = new Properties();
            ClassLoader classLoader = Utility.class.getClassLoader();
            properties.load(classLoader.getResourceAsStream(propertyName));
            return properties;
        } catch (IOException e) {
            throw new LoadConfigurationException(propertyName, e);
        }
    }
}
