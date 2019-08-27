package in.nimbo.service;

import java.util.Map;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class KeywordExtractorService {
    public static Map<String, Integer> extractKeywords(String text){
        Properties props;
        props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        props.setProperty("tokenize.language", "English");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        String[] stopwords = new String[]{"january", "february", "march", "april", "may", "june", "july", "august",
                "september", "october", "november", "december", "''", "--", "'s", "-lrb-", "-rrb-", "``", "pm", "am",
                "theme", "very", "about", "during", "when", "these", "would", "else", "above", "let", "because", "if",
                "you", "they", "between", "likely", "•", "in", "&", "want", "myself", "then", "it", "am", "yourselves",
                ",", "an", "each", ".", "ever", "as", "himself", "itself", "at", "among", "must", ":", "twa", "don",
                "other", "be", "against", "least", "ourselves", "out", "into", "across", "how", "same", "too", "get",
                "by", "have", "whom", "where", "after", "dear", "so", "a", "may", "more", "could", "i", "off", "...",
                "the", "such", "s", "t", "able", "to", "under", "yours", "through", "but", "theirs", "almost", "before",
                "own", "do", "while", "down", "that", "either", "ours", "than", "me", "only", "should", "few", "from",
                "yourself", "up", "those", "tis", "all", "which", "below", "like", "might", "this", "its", "often",
                "my", "both", "most", "she", "once", "herself", "since", "who", "however", "here", "no", "some",
                "rather", "for", "why", "we", "hers", "nor", "can", "not", "and", "now", "of", "themselves", "every",
                "just", "on", "over", "or", "will", "again", "yet", "say", "also", "any", "with", "what", "there",
                "neither", "until", "further", "he"
        };
        List<String> stop = Arrays.asList(stopwords);
        text = text.toLowerCase();
        Map<String, Integer> lemmas = new HashMap<>();
        CoreDocument document = new CoreDocument(text);
        pipeline.annotate(document);
        for (CoreMap token : document.tokens()) {
            String tok = token.get(CoreAnnotations.LemmaAnnotation.class);
            if (!stop.contains(tok) && tok.length() > 1 && !tok.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?"))
                lemmas.merge(tok, 1, Integer::sum);
        }
        return lemmas
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(comparingByValue()))
                .limit(5)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }
}