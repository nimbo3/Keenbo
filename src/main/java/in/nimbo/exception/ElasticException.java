package in.nimbo.exception;

public class ElasticException extends Exception {

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

    protected ElasticException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
