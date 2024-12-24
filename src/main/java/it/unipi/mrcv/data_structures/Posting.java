package it.unipi.mrcv.data_structures;

public class Posting {

    private int docid;
    private int frequency;

    // default constructor
    public Posting() {
    }

    // constructor that takes the docid and frequency as input
    public Posting(int docid, int frequency) {
        this.docid = docid;
        this.frequency = frequency;
    }

    // getters and setters
    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    // debug method to print the posting
    @Override
    public String toString() {
        return "Posting{docid=" + docid + ", frequency=" + frequency + '}';
    }

    // method to compare two postings
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Posting p)) {
            return false;
        }
        return docid == p.docid &&
                frequency == p.frequency;
    }
}