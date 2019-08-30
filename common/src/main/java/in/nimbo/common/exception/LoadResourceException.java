package in.nimbo.common.exception;

public class LoadResourceException extends RuntimeException {
    public LoadResourceException(String resourceFile, Throwable cause) {
        super("Unable to load file: " + resourceFile, cause);
    }
}
