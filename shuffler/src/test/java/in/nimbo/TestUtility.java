package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for do same work between tests
 */
public class TestUtility {
    private static Logger logger = LoggerFactory.getLogger(TestUtility.class);

    public static void setMetricRegistry() {
        if (SharedMetricRegistries.tryGetDefault() == null) {
            SharedMetricRegistries.setDefault("kafkaTest");
        }
    }
}
