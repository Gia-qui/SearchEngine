package it.unipi.mrcv.query;

import it.unipi.mrcv.data_structures.*;
import it.unipi.mrcv.data_structures.Comparators.DecComparatorDocument;
import it.unipi.mrcv.data_structures.Comparators.IncComparatorDocument;
import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.index.fileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.mrcv.global.Global.docLengths;

public class DAAT {
    public static PriorityQueue<Document> executeDAAT(ArrayList<String> queryTerms, int k) throws IOException {
        // Priority queues for documents, one for increasing and one for decreasing order
        PriorityQueue<Document> incQueue = new PriorityQueue<>(new IncComparatorDocument());
        PriorityQueue<Document> decQueue = new PriorityQueue<>(new DecComparatorDocument());
        // ArrayList to maintain the documents
        ArrayList<Document> documents = new ArrayList<>();
        // ArrayList to maintain posting lists and dictionary elems
        ArrayList<PostingList> postingLists = new ArrayList<>();
        ArrayList<DictionaryElem> dictionaryElems = new ArrayList<>();

        // load dictionary elements and posting lists
        for (String term : queryTerms) {
            DictionaryElem elem = DictionaryElem.binarySearch(term);
            if (elem != null) {
                PostingList pl = new PostingList(elem);
                postingLists.add(pl);
                dictionaryElems.add(elem);
            }
        }

        if (postingLists.isEmpty()) {
            return null;
        }

        // DAAT algorithm
        while (!postingLists.isEmpty()) {
            int minDocId = Integer.MAX_VALUE;
            for (int i = 0; i < postingLists.size(); i++) {
                Posting p = postingLists.get(i).getCurrent();
                if (p.getDocid() < minDocId) {
                    minDocId = p.getDocid();
                }
            }

            Document d = new Document(minDocId, 0);

            for (int i = 0; i < postingLists.size(); i++) {
                PostingList pl = postingLists.get(i);
                Posting p = pl.getCurrent();

                if (p.getDocid() == minDocId) {
                    if (Global.isBM25) {
                        d.calculateScoreBM25(dictionaryElems.get(i).getIdf(), p.getFrequency(), docLengths.get(minDocId));
                    } else {
                        d.calculateScoreTFIDF(dictionaryElems.get(i).getIdf(), p.getFrequency());
                    }

                    // Move to next posting in the list; if the posting list is exhausted, remove it from the list
                    if (pl.next() == null) {
                        postingLists.remove(i);
                        dictionaryElems.remove(i);
                        i--; // Adjust index due to removal
                    }

                }
            }

            // update the queues
            if (!documents.contains(d)) {
                if (documents.size() < k) {
                    documents.add(d);
                    incQueue.add(d);
                    decQueue.add(d);
                } else if (d.getScore() > incQueue.peek().getScore()) {
                    Document lowestScoreDoc = incQueue.poll();
                    decQueue.remove(lowestScoreDoc);
                    documents.remove(lowestScoreDoc);
                    documents.add(d);
                    incQueue.add(d);
                    decQueue.add(d);
                }
            }
        }

        return decQueue;
    }
}
