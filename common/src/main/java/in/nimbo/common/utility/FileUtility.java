package in.nimbo.common.utility;

import in.nimbo.common.exception.LoadResourceException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class FileUtility {
    private FileUtility() {}

    public static String readFileFromResource(String resource) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
            return new String(Files.readAllBytes(Paths.get(Objects.requireNonNull(url).toURI())), StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new LoadResourceException(resource, e);
        }
    }
}
