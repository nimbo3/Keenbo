package in.nimbo.service;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;
import in.nimbo.exception.LanguageDetectException;

import java.util.Arrays;

public class LangDetectService {

    public Language detectLanguage(String text) {
        try {
            DetectorFactory.loadProfile("profiles");
            Detector detector = DetectorFactory.create();
            detector.append(text);
            System.out.println(Arrays.toString(detector.getProbabilities().toArray()));
            return detector.getProbabilities().get(0);
        } catch (LangDetectException e) {
            throw new LanguageDetectException(e);
        } catch (IndexOutOfBoundsException e){
            throw new LanguageDetectException("Cannot detect language of input text", e);
        }
    }
}
