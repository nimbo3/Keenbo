package in.nimbo.exception;

public class LoadConfigurationException extends RuntimeException {
    public LoadConfigurationException(String configurationFile, Throwable cause) {
        super("Unable to load configuration file: " + configurationFile, cause);
    }
}
