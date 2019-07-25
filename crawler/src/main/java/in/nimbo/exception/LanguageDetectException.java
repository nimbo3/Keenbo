package in.nimbo.exception;

public class LanguageDetectException extends RuntimeException {
    public LanguageDetectException(Throwable e) {
        super(e);
    }

    public LanguageDetectException(String message, Throwable e){
        super(message, e);
    }
}
