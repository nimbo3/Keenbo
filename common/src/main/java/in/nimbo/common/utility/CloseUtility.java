package in.nimbo.common.utility;

public class CloseUtility {

    private CloseUtility() {
    }

    public static void closeSafely(AutoCloseable autoCloseable) {
        try {
            if (autoCloseable != null) {
                autoCloseable.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
