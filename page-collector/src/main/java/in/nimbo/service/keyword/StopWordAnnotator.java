package in.nimbo.service.keyword;

import edu.stanford.nlp.ling.CoreAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.ArraySet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class StopWordAnnotator implements Annotator {
    private List<String> stopWords;

    public StopWordAnnotator() {
        stopWords = Arrays.asList("january", "february", "march", "april", "may", "june", "july", "august", "-rcb-", "-rsb-",
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
                "neither", "until", "further", "he", "ago", "use");
    }

    @Override
    public void annotate(Annotation annotation) {
        for (CoreLabel token : annotation.get(CoreAnnotations.TokensAnnotation.class)) {
            if (stopWords.contains(token.word()) || token.word().length() == 1 ||
                    token.word().matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?")
                    || token.word().matches("\\d+(-|\\/)\\d+(-|\\/)\\d+")) {
                token.remove(CoreAnnotations.LemmaAnnotation.class);
            }
        }
    }

    @Override
    public Set<Class<? extends CoreAnnotation>> requirementsSatisfied() {
        return Collections.singleton(CoreAnnotations.LemmaAnnotation.class);
    }

    @Override
    public Set<Class<? extends CoreAnnotation>> requires() {
        return Collections.singleton(CoreAnnotations.LemmaAnnotation.class);
    }
}
