package in.nimbo.exception;

public class KafkaServiceException extends RuntimeException {
    public KafkaServiceException() {
        super();
    }

    public KafkaServiceException(String message) {
        super(message);
    }

    public KafkaServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaServiceException(Throwable cause) {
        super(cause);
    }
}
