package it.unipi.mrcv.preprocess;

import it.unipi.mrcv.global.Global;
import opennlp.tools.stemmer.PorterStemmer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class preprocess {
    public static ArrayList<String> all(String text) {
        ArrayList<String> ret;
        text = lowercase(text);
        text = text.replaceAll("https?://\\S+\\s?", " ");
        text = text.replaceAll("<[^>]*>", "");
        text = removePuntuaction(text);
        text = removeUnicode(text);
        text = text.replaceAll("\\s+", " "); //remove extra whitespaces
        ret = tokenize(text);
        if (Global.stopWords) {
            ret = stopWords(ret);
        }
        if (Global.stem) {
            ret = stem(ret);
        }
        ret = truncate(ret);
        return ret;
    }

    private static ArrayList<String> truncate(ArrayList<String> ret) {
        //truncate the token to max 40 characters
        return ret.stream()
                .map(token -> token.substring(0, Math.min(token.length(), 40)))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static ArrayList<String> stopWords(ArrayList<String> tokens) {
        return tokens.stream()
                .filter(token -> !Global.stopWordsSet.contains(token))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public static ArrayList<String> stem(ArrayList<String> tokens) {
        PorterStemmer porterStemmer = new PorterStemmer();
        ArrayList<String> ret = new ArrayList<>();
        for (String token : tokens) {
            String stem = porterStemmer.stem(token);
            //System.out.println("Token: " + token + " - Stem: " + stem);
            ret.add(stem);
        }
        return ret;
    }

    public static ArrayList<String> tokenize(String text) {

        return Stream.of(text.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }

    public static String lowercase(String text) {
        return text.toLowerCase();
    }

    public static String removePuntuaction(String text) {
        String result = text.replaceAll("\\p{Punct}", " ");
        return result;
    }

    public static String removeUnicode(String text) {
        String str;
        byte[] strBytes = text.getBytes(StandardCharsets.UTF_8);

        str = new String(strBytes, StandardCharsets.UTF_8);

        Pattern unicodeOutliers = Pattern.compile("[^\\x00-\\x7F]",
                Pattern.UNICODE_CASE | Pattern.CANON_EQ
                        | Pattern.CASE_INSENSITIVE);

        Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(str);
        str = unicodeOutlierMatcher.replaceAll(" ");

        return str;
    }
}
