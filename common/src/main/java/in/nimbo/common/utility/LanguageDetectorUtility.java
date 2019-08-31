package in.nimbo.common.utility;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import in.nimbo.common.exception.LanguageDetectException;
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

    public static boolean isEnglishLanguage(String text, double englishProbability) {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            detector.setAlpha(0);
            return detector.getProbabilities().stream()
                    .anyMatch(x -> x.lang.equals("en") && x.prob > englishProbability);
        } catch (LangDetectException e) {
            throw new LanguageDetectException(e);
        }
    }
}
