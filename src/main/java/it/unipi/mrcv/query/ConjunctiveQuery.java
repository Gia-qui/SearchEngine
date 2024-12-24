package it.unipi.mrcv.query;

import it.unipi.mrcv.data_structures.*;
import it.unipi.mrcv.data_structures.Comparators.IncComparatorDocument;
import it.unipi.mrcv.data_structures.Comparators.DecComparatorDocument;
import it.unipi.mrcv.global.Global;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import static it.unipi.mrcv.global.Global.docIdsChannel;
import static it.unipi.mrcv.global.Global.docLengths;

public class ConjunctiveQuery {

    public static PriorityQueue<Document> executeConjunctiveQuery(ArrayList<String> queryTerms, int k) throws IOException {

        // ArrayList to maintain ordered posting lists based on length
        ArrayList<PostingList> postingLists = new ArrayList<>();
        // Arraylist of dictionary elems
        ArrayList<DictionaryElem> dictionaryElems = new ArrayList<>();

        // Load PostingLists and their lengths for each term in the query
        for (String term : queryTerms) {
            DictionaryElem elem = DictionaryElem.binarySearch(term);
            if (elem != null) {
                dictionaryElems.add(elem);
            }
        }

        // Sort the dictionary elems by increasing df
        dictionaryElems.sort((o1, o2) -> {
            if (o1.getDf() < o2.getDf()) {
                return -1;
            } else if (o1.getDf() > o2.getDf()) {
                return 1;
            } else {
                return 0;
            }
        });
        // Sort posting lists by df
        for (DictionaryElem elem : dictionaryElems) {
            postingLists.add(new PostingList(elem));
        }

        // Priority queues for documents, one for increasing and one for decreasing order
        PriorityQueue<Document> incPQueue = new PriorityQueue<>(k, new IncComparatorDocument());
        PriorityQueue<Document> decPQueue = new PriorityQueue<>(k, new DecComparatorDocument());

        // First docID from the shortest posting list
        Posting currentPosting = postingLists.get(0).getCurrent();


        // Loop until all the docIDs of the first posting list have been processed
        while (currentPosting != null) {
            int currentDocId = currentPosting.getDocid();
            if (checkAllPostingList(postingLists, currentDocId)) {
                // Create a new document
                Document doc = new Document(currentDocId, 0);

                // Compute score
                for (int i = 0; i < postingLists.size(); i++) {
                    Posting p = postingLists.get(i).getCurrent();

                    // check the flag for the scoring method
                    if (Global.isBM25) {
                        doc.calculateScoreBM25(dictionaryElems.get(i).getIdf(), p.getFrequency(), docLengths.get(currentDocId));
                    } else {
                        doc.calculateScoreTFIDF(dictionaryElems.get(i).getIdf(), p.getFrequency());
                    }
                }

                // Add to both queues
                incPQueue.add(doc);
                decPQueue.add(doc);

                // If we've collected enough documents, remove the lowest scoring one
                if (incPQueue.size() > k) {
                    Document removed = incPQueue.poll();
                    decPQueue.remove(removed);
                }
            }
            // Move to the next document in the shortest list
            currentPosting = postingLists.get(0).next();
        }

        return decPQueue;
    }

    private static boolean checkAllPostingList(ArrayList<PostingList> postingLists, int docId) throws IOException {
        // Check if docId is present in all posting lists
        for (PostingList pl : postingLists) {
            Posting p = pl.nextGEQ(docId);
            if (p == null || p.getDocid() != docId) {
                return false;
            }
        }
        return true;
    }
}
