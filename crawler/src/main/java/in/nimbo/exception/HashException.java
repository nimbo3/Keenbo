package in.nimbo.exception;

public class HashException extends RuntimeException {
    public HashException() {
        super();
    }

    public HashException(String message) {
        super(message);
    }

    public HashException(String message, Throwable cause) {
        super(message, cause);
    }

    public HashException(Throwable cause) {
        super(cause);
    }
}
