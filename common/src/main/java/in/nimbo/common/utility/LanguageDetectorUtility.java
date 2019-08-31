package in.nimbo.common.utility;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.slf4j.Logger;

public class LanguageDetectorUtility {
    private LanguageDetectorUtility() {}

    public static void loadLanguageDetector(Logger logger) {
        try {
            logger.info("Load application profiles for language detector");
            DetectorFactory.loadProfile("../conf/profiles");
        } catch (LangDetectException e) {
            logger.info("Unable to load profiles of language detector. Provide \"profile\" folder for language detector.\n");
            System.exit(1);
        }
    }
}
