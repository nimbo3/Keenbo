package in.nimbo.common.exception;

public class ParseLinkException extends RuntimeException {
    public ParseLinkException() {
        super();
    }

    public ParseLinkException(String message) {
        super(message);
    }

    public ParseLinkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParseLinkException(Throwable cause) {
        super(cause);
    }
}
