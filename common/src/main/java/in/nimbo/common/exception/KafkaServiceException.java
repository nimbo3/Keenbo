package in.nimbo.common.exception;

public class KafkaServiceException extends RuntimeException {
    public KafkaServiceException() {
        super();
    }

    public KafkaServiceException(Throwable cause) {
        super(cause);
    }
}
