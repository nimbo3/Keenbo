package in.nimbo.service.keyword;

import edu.stanford.nlp.simple.Sentence;

import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class KeywordExtractorService {
    private static List<String> stopWords = Arrays.asList("january", "february", "march", "april", "may", "june", "july",
            "september", "october", "november", "december", "''", "--", "'s", "-lrb-", "-rrb-", "``", "pm", "am", "-rcb-",
            "theme", "very", "about", "during", "when", "these", "would", "else", "above", "let", "because", "if", "august",
            "you", "they", "between", "likely", "•", "in", "&", "want", "myself", "then", "it", "am", "yourselves", "-rsb-",
            ",", "an", "each", ".", "ever", "as", "himself", "itself", "at", "among", "must", ":", "twa", "don", "-lsb-",
            "other", "be", "against", "least", "ourselves", "out", "into", "across", "how", "same", "too", "get",
            "by", "have", "whom", "where", "after", "dear", "so", "may", "more", "could", "off", "...",
            "the", "such", "able", "to", "under", "yours", "through", "but", "theirs", "almost", "before",
            "own", "do", "while", "down", "that", "either", "ours", "than", "me", "only", "should", "few", "from",
            "yourself", "up", "those", "tis", "all", "which", "below", "like", "might", "this", "its", "often",
            "my", "both", "most", "she", "once", "herself", "since", "who", "however", "here", "no", "some",
            "rather", "for", "why", "we", "hers", "nor", "can", "not", "and", "now", "of", "themselves", "every",
            "just", "on", "over", "or", "will", "again", "yet", "say", "also", "any", "with", "what", "there",
            "neither", "until", "further", "he", "ago", "use", "-lcb-");

    public static Map<String, Integer> extractKeywords(String text) {
        text = text.toLowerCase().replaceAll("\\.", " ");
        Sentence sentence = new Sentence(text);
        Map<String, Integer> lemmas = new HashMap<>();
        for (String lemma : sentence.lemmas()) {
            if (!isStopWord(lemma)) {
                lemmas.merge(lemma, 1, Integer::sum);
            }
        }
        return lemmas
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(comparingByValue()))
                .limit(5)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }

    private static boolean isStopWord(String word) {
        return stopWords.contains(word) || word.length() == 1 ||
                word.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?")
                || word.matches("\\d+(-|\\/)\\d+(-|\\/)\\d+");
    }
}