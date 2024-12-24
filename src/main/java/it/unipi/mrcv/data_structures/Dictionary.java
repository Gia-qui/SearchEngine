package it.unipi.mrcv.data_structures;

import java.util.TreeMap;

public class Dictionary {
    private final TreeMap<String, DictionaryElem> dictionary;

    // default constructor
    public Dictionary() {
        dictionary = new TreeMap<>();
    }

    // insert element in the dictionary
    public void insertElem(DictionaryElem elem) {
        dictionary.put(elem.getTerm(), elem);
    }

    // return element from the dictionary
    public DictionaryElem getElem(String term) {
        return dictionary.get(term);
    }

    // return the size in bytes of the partial dictionary before the merger
    public long SPIMIsize() {
        return (long) dictionary.size() * DictionaryElem.SPIMIsize();

    }

    // return the length of the dictionary
    public long length() {
        return dictionary.size();

    }

    // method to clear the dictionary
    public void clear() {
        dictionary.clear();
    }

}
