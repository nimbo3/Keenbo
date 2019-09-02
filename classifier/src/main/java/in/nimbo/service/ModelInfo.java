package in.nimbo.service;

import java.util.HashMap;
import java.util.Map;

public class ModelInfo {
    private String[] stopWords;
    private Map<Double, String> labelMap;

    public ModelInfo() {
        labelMap = new HashMap<>();
        labelMap.put(0.0, "art");
        labelMap.put(1.0, "science");
        labelMap.put(2.0, "health");
        labelMap.put(3.0, "news");
        labelMap.put(4.0, "shopping");
        labelMap.put(5.0, "sports");
        labelMap.put(6.0, "social");
        labelMap.put(7.0, "other");

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

    public String getLabelString(Double num) {
        return labelMap.get(num);
    }

    public Double getLabelDouble(String name) {
        for (Double num : labelMap.keySet()) {
            if (labelMap.get(num).equals(name))
                return num;
        }
        return 0.0;
    }
}
