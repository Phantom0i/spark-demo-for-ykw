package me.demo.operator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class StopWordsFilter {
    private static final String STOP_WORD_RESOURCE_NAME = "stop-words.txt";
    private Set<String> stopWords = new HashSet<>();

    private StopWordsFilter() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(STOP_WORD_RESOURCE_NAME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        reader.lines().forEach(line -> stopWords.add(line.trim()));
    }

    private static class Holder {
        private static StopWordsFilter instance = new StopWordsFilter();
    }

    public static StopWordsFilter getInstance() {
        return Holder.instance;
    }

    public boolean isStopWord(String word) {
        return stopWords.contains(word);
    }

    public Stream<String> filter(Stream<String> words) {
        return words.filter(s -> !stopWords.contains(s));
    }
}
