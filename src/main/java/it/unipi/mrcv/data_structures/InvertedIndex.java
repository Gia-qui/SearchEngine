package it.unipi.mrcv.data_structures;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;

// This class represents the inverted index
public class InvertedIndex {

    private final TreeMap<String, PostingList> index;

    public InvertedIndex() {
        index = new TreeMap<>();
    }

    public void addPosting(String term, Posting posting) {
        // Check if the term is already in the index
        if (index.containsKey(term)) {
            PostingList postingList = index.get(term);
            postingList.addPosting(posting);
        } else {
            PostingList postingList = new PostingList();
            postingList.addPosting(posting);
            index.put(term, postingList);
        }
    }

    public PostingList getPostings(String term) {
        return index.getOrDefault(term, new PostingList());
    }

    public List<String> getTerms() {
        return new ArrayList<>(index.keySet());
    }

    public TreeMap<String, PostingList> getTree() {
        return index;
    }

    // method to clear the index
    public void clear() {
        index.clear();
    }
}