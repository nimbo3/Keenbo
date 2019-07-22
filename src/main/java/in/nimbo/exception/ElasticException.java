package in.nimbo.exception;

public class ElasticException extends RuntimeException {

    public ElasticException() {
        super();
    }

    public ElasticException(String message) {
        super(message);
    }

    public ElasticException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticException(Throwable cause) {
        super(cause);
    }
}
