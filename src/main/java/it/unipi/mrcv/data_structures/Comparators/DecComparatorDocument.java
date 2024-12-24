package it.unipi.mrcv.data_structures.Comparators;

import it.unipi.mrcv.data_structures.Document;

// comparator for the Document class used in the priority queue for the merger
public class DecComparatorDocument implements java.util.Comparator<Document> {
    // compare two Document objects; higher score first, if the scores are the same, compare the docIds
    @Override
    public int compare(Document o1, Document o2) {
        if (o1.getScore() < o2.getScore()) {
            return 1;
        } else if (o1.getScore() > o2.getScore()) {
            return -1;
        } else {
            if (o1.getDocId() < o2.getDocId())
                return 1;
            else
                return -1;
        }
    }
}
