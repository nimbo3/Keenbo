package in.nimbo.common.utility;

import in.nimbo.common.exception.LoadResourceException;

import java.io.InputStream;
import java.util.Objects;
import java.util.Scanner;

public class FileUtility {
    private FileUtility() {}

    public static String readFileFromResource(String resource) {
        try {
            InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            Scanner s = new Scanner(Objects.requireNonNull(resourceAsStream)).useDelimiter("\\A");
            return s.hasNext() ? s.next() : "";
        } catch (NullPointerException e) {
            throw new LoadResourceException(resource, e);
        }
    }
}
