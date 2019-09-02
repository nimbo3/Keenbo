package in.nimbo.service;

import java.util.HashMap;
import java.util.Map;

public class ModelInfo {
    private String[] stopWords;
    private Map<String, Double> labelMap;

    public ModelInfo() {
        labelMap = new HashMap<>();
        labelMap.put("art", 0.0);
        labelMap.put("science", 1.0);
        labelMap.put("health", 2.0);
        labelMap.put("news", 3.0);
        labelMap.put("shopping", 4.0);
        labelMap.put("sports", 5.0);
        labelMap.put("social", 6.0);
        labelMap.put("other", 7.0);

        stopWords = new String[]{"'", "-", "'s", "``", "pm", "am", "their", "our",
                "theme", "very", "about", "during", "when", "these", "would", "else", "above", "let", "because", "if",
                "you", "they", "between", "likely", "•", "in", "&", "want", "myself", "then", "it", "am", "yourselves",
                "an", "each", "ever", "as", "himself", "itself", "at", "among", "must", ":", "twa", "don",
                "other", "is", "am", "are", "against", "least", "ourselves", "out", "into", "across", "how", "same", "too", "get",
                "by", "have", "whom", "where", "after", "dear", "so", "may", "more", "could", "off", "...",
                "the", "such", "able", "to", "under", "yours", "through", "but", "theirs", "almost", "before",
                "own", "do", "while", "down", "that", "either", "ours", "than", "me", "only", "should", "few", "from",
                "yourself", "up", "those", "tis", "all", "which", "below", "like", "might", "this", "its", "often",
                "my", "both", "most", "she", "once", "herself", "since", "who", "however", "here", "no", "some",
                "rather", "for", "why", "we", "hers", "nor", "can", "not", "and", "now", "of", "themselves", "every",
                "just", "on", "over", "or", "will", "again", "yet", "say", "also", "any", "with", "what", "there",
                "neither", "until", "further", "he", "ago", "use", "-lcb-", "your", "per", "lot"};
    }

    public String[] getStopWords() {
        return stopWords;
    }

    public Map<String, Double> getLabelMap() {
        return labelMap;
    }
}
