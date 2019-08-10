package in.nimbo.common.exception;

public class CompressionException extends RuntimeException {
    public CompressionException() {
        super();
    }

    public CompressionException(String message) {
        super(message);
    }

    public CompressionException(String message, Throwable cause) {
        super(message, cause);
    }

    public CompressionException(Throwable cause) {
        super(cause);
    }
}
