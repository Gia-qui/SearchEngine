package it.unipi.mrcv.data_structures;

import it.unipi.mrcv.compression.Unary;
import it.unipi.mrcv.compression.VariableByte;
import it.unipi.mrcv.global.Global;
import it.unipi.mrcv.index.fileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.unipi.mrcv.global.Global.compression;

public class PostingList {
    // list of postings
    private final ArrayList<Posting> postings;
    // list of skip elements relative to the posting list
    public ArrayList<SkipElem> skipElems = null;
    // current block iterator
    public int currentBlock = -1;
    // current position in the posting list
    public int currentPosition = -1;
    // term
    private String term;

    // default constructor
    public PostingList() {
        this.term = " ";
        this.postings = new ArrayList<>();

    }

    public PostingList(String term) {
        this.term = term;
        this.postings = new ArrayList<>();
    }

    public PostingList(String term, Posting p) {
        this.term = term;
        this.postings = new ArrayList<>();
        this.postings.add(p);
    }

    // constructor that takes a dictionary element as input to initialize the whole posting list (or the first block)
    public PostingList(DictionaryElem elem) throws IOException {
        this.term = elem.getTerm();
        this.postings = new ArrayList<>();
        currentPosition = 0;
        if (compression) {
            if (elem.getSkipLen() != 0) {
                this.skipElems = SkipElem.readSkipList(elem.getOffsetSkip(), elem.getSkipLen());
                loadBlock(skipElems.get(0).getOffsetDoc(),
                        skipElems.get(0).getOffsetFreq(),
                        skipElems.get(0).getDocBlockLen(),
                        skipElems.get(0).getFreqBlockLen());
            } else {
                loadBlock(elem.getOffsetDoc(), elem.getOffsetFreq(), elem.getLengthDocIds(), elem.getLengthFreq());
            }
            currentBlock++;
        } else {
            this.addPostings(fileUtils.readPosting(elem.getOffsetDoc(), elem.getLengthDocIds()));
        }
    }

    public void addPosting(Posting posting) {
        postings.add(posting);
    }

    public void addPostings(ArrayList<Posting> postings) {
        this.postings.addAll(postings);
    }

    public List<Posting> getPostings() {
        return postings;
    }

    public int size() {
        return postings.size();
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void printPostingList() {
        System.out.println("Posting List:");
        for (Posting p : postings) {
            System.out.printf("Docid: %d - Freq: %d\n", p.getDocid(), p.getFrequency());
        }
    }

    // method to load a block of the posting list given the offsets and the length
    private void loadBlock(long offsetDoc, long offsetFreq, int lengthDoc, int lenghtFreq) throws IOException {
        postings.clear();
        if (compression) {

            byte[] docsBytes = fileUtils.readCompressed(Global.docIdsChannel, offsetDoc, lengthDoc);
            byte[] freqsBytes = fileUtils.readCompressed(Global.frequenciesChannel, offsetFreq, lenghtFreq);


            ArrayList<Integer> docIds = VariableByte.fromByteToArrayInt(docsBytes);

            ArrayList<Integer> freqs = Unary.unaryToArrayInt(freqsBytes);

            for (int i = 0; i < docIds.size(); i++) {
                int docId = docIds.get(i);
                int freq = freqs.get(i);
                addPosting(new Posting(docId, freq));
            }

        }

    }

    // method that returns the current posting of the iterator
    public Posting getCurrent() {
        if (currentPosition == -1) {
            return null;
        }
        if (currentPosition < postings.size()) {
            return postings.get(currentPosition);
        } else {
            return null;
        }
    }

    // method that returns the next posting of the iterator and updates the current position
    public Posting next() {
        if (currentPosition + 1 < postings.size()) {
            currentPosition++;
            return postings.get(currentPosition);
        } else { //block finished
            if (skipElems != null) {
                currentBlock++;
                if (currentBlock < skipElems.size()) {
                    currentPosition = 0;
                    try {
                        loadBlock(skipElems.get(currentBlock).getOffsetDoc(), skipElems.get(currentBlock).getOffsetFreq(), skipElems.get(currentBlock).getDocBlockLen(), skipElems.get(currentBlock).getFreqBlockLen());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return postings.get(currentPosition);
                } else {
                    currentPosition = -1; //finished the pl
                    return null;
                }
            } else {
                currentPosition = -1;
                return null;
            }
        }
    }

    // nextGEQ returns the first posting with docid greater or equal to docid (if it exists)
    // it performs binary search on the blocks and then on the postings inside the block -> O(log(sqrt(n)) * log(sqrt(n)))
    public Posting nextGEQ(int docid) {

        // check if the blocks are finished
        if (skipElems != null && currentBlock >= skipElems.size()) {
            return null;
        }
        // check if the posting list is empty
        if (this.getCurrent() == null) {
            return null;
        }
        // check that the currentId is not greater than the requested one
        if (this.getCurrent().getDocid() >= docid) {
            return this.getCurrent();
        }

        // check which block is the one we are looking for and load it if there is a skip list
        if (skipElems != null && skipElems.get(currentBlock).getDocID() <= docid) {

            // check if there is not a GEQ in the posting list
            if (skipElems.get(skipElems.size() - 1).getDocID() < docid) {
                currentPosition = -1;
                return null;
            }

            // BINARY SEARCH OF THE BLOCK
            int low = currentBlock;
            int end = skipElems.size();
            int mid;

            while (low < end) {
                mid = low + ((end - low) >>> 1);

                if (skipElems.get(mid).getDocID() < docid) {
                    low = mid + 1;
                } else {
                    end = mid;
                }
            }

            if (low < skipElems.size() && skipElems.get(low).getDocID() >= docid) {
                currentBlock = low;
            } else {
                currentPosition = -1;
                return null;
            }

            // load the block
            currentPosition = 0;
            try {
                loadBlock(skipElems.get(currentBlock).getOffsetDoc(), skipElems.get(currentBlock).getOffsetFreq(), skipElems.get(currentBlock).getDocBlockLen(), skipElems.get(currentBlock).getFreqBlockLen());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        // if there is not a skip list, we have to check if the last posting is greater than the requested one
        else {
            if (postings.get(postings.size() - 1).getDocid() < docid) {
                return null;
            }
        }
        if (postings.isEmpty()) {
            return null;
        }

        // BINARY SEARCH OF THE DOCID
        int low = currentPosition;
        int high = postings.size() - 1;

        while (low <= high) {
            // bitwise shift to divide by 2
            int mid = (low + high) >>> 1;
            Posting midPosting = postings.get(mid);
            int midDocId = midPosting.getDocid();
            if (midDocId < docid) {
                low = mid + 1;
            } else if (midDocId > docid) {
                high = mid - 1;
            } else {
                currentPosition = mid;
                return midPosting;
            }
        }

        if (low < postings.size()) {
            currentPosition = low;
            return postings.get(low);
        } else {
            return null;
        }
    }


}
