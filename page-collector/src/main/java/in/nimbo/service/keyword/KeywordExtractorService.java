package in.nimbo.service.keyword;

import java.util.Map;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class KeywordExtractorService {
    public static Map<String, Integer> extractKeywords(String text) {
        Properties props;
        props = new Properties();
        props.setProperty("tokenize.language", "English");
        props.setProperty("customAnnotatorClass.stopword", "in.nimbo.service.keyword.StopWordAnnotator");
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, stopword");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        text = text.toLowerCase();
        Map<String, Integer> lemmas = new HashMap<>();
        CoreDocument document = new CoreDocument(text);
        pipeline.annotate(document);
        for (CoreMap token : document.tokens()) {
            String tok = token.get(CoreAnnotations.LemmaAnnotation.class);
            lemmas.merge(tok, 1, Integer::sum);
        }
        return lemmas
                .entrySet()
                .stream()
                .filter(x -> x.getKey() != null)
                .sorted(Collections.reverseOrder(comparingByValue()))
                .limit(5)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }
}