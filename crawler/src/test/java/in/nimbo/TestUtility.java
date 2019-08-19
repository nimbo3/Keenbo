package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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

    /**
     * get content a file as string
     *
     * @param path path of file
     * @return content of file
     * @throws RuntimeException if unable to get content of file
     */
    public static String getFileContent(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Couldn't read file: " + path, e);
            throw new RuntimeException("Couldn't read file: " + path, e);
        }
    }
}
