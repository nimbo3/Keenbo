package in.nimbo.service.keyword;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class KeywordExtractorService {
    private List<String> stopWords;
    private final POSTaggerME posTagger;
    private final DictionaryLemmatizer lemmatizer;

    public KeywordExtractorService() throws IOException {
        stopWords = Arrays.asList("january", "february", "march", "april", "may", "june", "july", "much", "their", "our", "seem",
                "september", "october", "november", "december", "''", "--", "'s", "-lrb-", "-rrb-", "``", "pm", "am", "-rcb-",
                "theme", "very", "about", "during", "when", "these", "would", "else", "above", "let", "because", "if", "august",
                "you", "they", "between", "likely", "•", "in", "&", "want", "myself", "then", "it", "am", "yourselves", "-rsb-",
                "an", "each", "ever", "as", "himself", "itself", "at", "among", "must", ":", "twa", "don", "-lsb-",
                "other", "be", "against", "least", "ourselves", "out", "into", "across", "how", "same", "too", "get",
                "by", "have", "whom", "where", "after", "dear", "so", "may", "more", "could", "off", "...",
                "the", "such", "able", "to", "under", "yours", "through", "but", "theirs", "almost", "before",
                "own", "do", "while", "down", "that", "either", "ours", "than", "me", "only", "should", "few", "from",
                "yourself", "up", "those", "tis", "all", "which", "below", "like", "might", "this", "its", "often",
                "my", "both", "most", "she", "once", "herself", "since", "who", "however", "here", "no", "some",
                "rather", "for", "why", "we", "hers", "nor", "can", "not", "and", "now", "of", "themselves", "every",
                "just", "on", "over", "or", "will", "again", "yet", "say", "also", "any", "with", "what", "there",
                "neither", "until", "further", "he", "ago", "use", "-lcb-", "your", "per", "lot");
        InputStream posModel = Thread.currentThread().getContextClassLoader().getResourceAsStream("data/en-pos-maxent.bin");
        InputStream lemmatizerDict = Thread.currentThread().getContextClassLoader().getResourceAsStream("data/en-lemmatizer.dict");
        posTagger = new POSTaggerME(new POSModel(Objects.requireNonNull(posModel)));
        lemmatizer = new DictionaryLemmatizer(Objects.requireNonNull(lemmatizerDict));
    }

    public Map<String, Integer> extractKeywords(String text) {
        String[] tokens = text.toLowerCase().replaceAll("\\.|\\?|\\(|\\)|\\-|:|;|&|=|\\+|\\*|'|,|\\/", " ").split("\\s+");
        String[] tags;
        String[] lemma;
        synchronized (posTagger) {
            tags = posTagger.tag(tokens);
        }
        synchronized (lemmatizer) {
            lemma = lemmatizer.lemmatize(tokens, tags);
        }
        Map<String, Integer> lemmas = new HashMap<>();
        for (int i = 0; i < tokens.length; i++) {
            String val;
            if (lemma[i].equals("O")) {
                val = tokens[i];
            } else {
                val = lemma[i];
            }
            if (!isStopWord(val)) {
                lemmas.merge(val, 1, Integer::sum);
            }
        }
        return lemmas
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(comparingByValue()))
                .limit(5)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }

    private boolean isStopWord(String word) {
        return stopWords.contains(word) || word.length() == 1 ||
                word.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?") ||
                word.matches("https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)");
    }
}