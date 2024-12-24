package it.unipi.mrcv.query;

import it.unipi.mrcv.data_structures.*;
import it.unipi.mrcv.data_structures.Comparators.DecComparatorDocument;
import it.unipi.mrcv.data_structures.Comparators.IncComparatorDocument;
import it.unipi.mrcv.global.Global;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.mrcv.data_structures.Document.calculateScoreStaticBM25;
import static it.unipi.mrcv.data_structures.Document.calculateScoreStaticTFIDF;
import static it.unipi.mrcv.global.Global.docLengths;

public class MaxScore {

    public static PriorityQueue<Document> executeMaxScore(ArrayList<String> queryTerms, int k) throws IOException {
        // Priority queues for documents
        PriorityQueue<Document> incQueue = new PriorityQueue<>(k, new IncComparatorDocument());
        PriorityQueue<Document> decQueue = new PriorityQueue<>(k, new DecComparatorDocument());
        // Load PostingLists and DictionaryElems for each query term
        ArrayList<PostingList> postingLists = new ArrayList<>();
        ArrayList<DictionaryElem> dictionaryElems = new ArrayList<>();
        for (String term : queryTerms) {
            DictionaryElem elem = DictionaryElem.binarySearch(term);
            if (elem != null) {
                dictionaryElems.add(elem);
            }
        }
        // Sort the lists based on term contribution (maxTFIDF or maxBM25)
        Collections.sort(dictionaryElems, new Comparator<DictionaryElem>() {
            @Override
            public int compare(DictionaryElem o1, DictionaryElem o2) {
                if (!Global.isBM25)
                    return Double.compare(o1.getMaxTFIDF(), o2.getMaxTFIDF());
                else
                    return Double.compare(o1.getMaxBM25(), o2.getMaxBM25());
            }
        });
        // Sort PostingList in the same order as dictionaryElems
        for (DictionaryElem elem : dictionaryElems) {
            postingLists.add(new PostingList(elem));
        }


        // Compute the upper bounds and initialize variables
        ArrayList<Double> upperBounds = computeUpperBounds(dictionaryElems);
        int pivot = 0;
        int next;
        double threshold = 0;
        int currentDocId = getMinimumDocId(postingLists);

        // Process documents
        while (pivot < postingLists.size() && currentDocId != -1) {
            //System.out.println(pivot);
            double score = 0;
            next = Global.collectionLength;

            // Process the essential lists
            for (int i = pivot; i < postingLists.size(); i++) {
                PostingList pl = postingLists.get(i);
                Posting p = pl.getCurrent();
                if (p != null && p.getDocid() == currentDocId) {
                    score += calculateScore(p, dictionaryElems.get(i));
                    p = pl.next();
                }
                if (p != null && p.getDocid() < next) {
                    next = p.getDocid();
                }

            }

            // Process the non-essential lists
            for (int i = pivot - 1; i >= 0; i--) {
                if (score + upperBounds.get(i) <= threshold) {
                    break;
                }
                PostingList pl = postingLists.get(i);
                Posting p = pl.nextGEQ(currentDocId);
                if (p != null && p.getDocid() == currentDocId) {
                    score += calculateScore(p, dictionaryElems.get(i));

                }
            }

            // Update the threshold and pivot
            if (incQueue.size() < k || score > incQueue.peek().getScore()) {
                //System.out.println(currentDocId);
                Document d = new Document(currentDocId, score);
                incQueue.add(d);
                decQueue.add(d);
                if (incQueue.size() > k) {
                    Document removed = incQueue.poll();
                    decQueue.remove(removed);
                }
                threshold = incQueue.peek().getScore();
                while (pivot < postingLists.size() && upperBounds.get(pivot) <= threshold) {
                    pivot++;
                }
            }

            // Find the next document to process
            if (next == Global.collectionLength)
                currentDocId = -1;
            else if (next > currentDocId)
                currentDocId = next;

        }
        return decQueue;
    }

    private static ArrayList<Double> computeUpperBounds(ArrayList<DictionaryElem> dictionaryElems) {
        ArrayList<Double> upperBounds = new ArrayList<>();
        double cumulativeUpperBound = 0.0;

        for (DictionaryElem elem : dictionaryElems) {
            if (!Global.isBM25)
                cumulativeUpperBound += elem.getMaxTFIDF();
            else
                cumulativeUpperBound += elem.getMaxBM25();
            upperBounds.add(cumulativeUpperBound);
        }
        return upperBounds;
    }

    private static int getMinimumDocId(ArrayList<PostingList> postingLists) {
        Posting minPosting = null;
        int minDocId = Integer.MAX_VALUE;
        for (PostingList pl : postingLists) {
            Posting current = pl.getCurrent();
            if (current != null && current.getDocid() < minDocId) {
                minDocId = current.getDocid();
                minPosting = current;
            }
        }
        if (minPosting == null)
            return -1;
        else
            return minPosting.getDocid();
    }

    private static double calculateScore(Posting posting, DictionaryElem dictionaryElem) {

        if (!Global.isBM25)
            return calculateScoreStaticTFIDF(dictionaryElem.getIdf(), posting.getFrequency());
        else
            return calculateScoreStaticBM25(dictionaryElem.getIdf(), posting.getFrequency(), docLengths.get(posting.getDocid()));

    }
}
