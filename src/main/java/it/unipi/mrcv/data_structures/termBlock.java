package it.unipi.mrcv.data_structures;

// class to store the term and the number of the block in which the term is stored used in the merger pqueue
public class termBlock {
    // dictionaryElem of the term
    private final DictionaryElem term;
    // number of the block in which the term is stored
    private int numBlock;

    // constructor which allows to set the term and the number of the block
    public termBlock(String t, int n) {
        this.term = new DictionaryElem(t);
        this.numBlock = n;
    }

    // constructor which allows to set every value of the termBlock
    public termBlock(String t, int df, int cf, long offsetDoc, long offsetFreq, int length, int numBlock) {
        this.term = new DictionaryElem(t);
        term.setDf(df);
        term.setCf(cf);
        term.setOffsetDoc(offsetDoc);
        term.setOffsetFreq(offsetFreq);
        term.setLengthDocIds(length);
        this.numBlock = numBlock;
    }

    // default constructor
    public termBlock() {
        this.term = new DictionaryElem();
        this.numBlock = 0;
    }

    // setters and getters
    public String getTerm() {
        return this.term.getTerm();
    }

    public int getNumBlock() {
        return this.numBlock;
    }

    public void setNumBlock(int numBlock) {
        this.numBlock = numBlock;
    }

    public DictionaryElem getDictionaryElem() {
        return this.term;
    }

    // method to copy the values of a termBlock into another one
    public void copyBlock(termBlock t) {
        this.term.setTerm(t.getTerm());
        this.term.setDf(t.getDictionaryElem().getDf());
        this.term.setCf(t.getDictionaryElem().getCf());
        this.term.setOffsetDoc(t.getDictionaryElem().getOffsetDoc());
        this.term.setOffsetFreq(t.getDictionaryElem().getOffsetFreq());
        this.term.setLengthDocIds(t.getDictionaryElem().getLengthDocIds());
        this.term.setLengthFreq(t.getDictionaryElem().getLengthFreq());
        this.numBlock = t.getNumBlock();

        this.term.setMaxTF(0);
        this.term.setOffsetSkip(0);
        this.term.setSkipLen(0);
        this.term.setIdf(0);
        this.term.setMaxTFIDF(0);
        this.term.setMaxBM25(0);

    }
}
