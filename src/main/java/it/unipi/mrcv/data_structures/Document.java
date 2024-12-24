package it.unipi.mrcv.data_structures;

import static it.unipi.mrcv.global.Global.averageDocLength;
import static it.unipi.mrcv.global.Global.collectionLength;

// class used during query processing to store docId and partial score of a document
public class Document {
    int docId;
    double score;

    public Document(int docId, double score) {
        this.docId = docId;
        this.score = score;
    }

    // static methods used in conjunctive query processing and MaxScore
    public static double calculateScoreStaticBM25(double idf, int tf, int docLength) {
        double k1 = 1.2;
        double b = 0.75;
        double numerator = tf * (k1 + 1);
        double denominator = tf + k1 * (1 - b + b * (docLength / averageDocLength));
        return idf * numerator / denominator;


    }

    public static double calculateScoreStaticTFIDF(double idf, int tf) {
        return idf * (1 + Math.log10(tf));
    }

    public int getDocId() {
        return docId;
    }

    // methods used in DAAT query processing to increment partial score
    public void calculateScoreBM25(double idf, int tf, int docLength) {
        double k1 = 1.2;
        double b = 0.75;
        double numerator = tf * (k1 + 1);
        double denominator = tf + k1 * (1 - b + b * (docLength / averageDocLength));
        this.score += idf * numerator / denominator;
    }

    public void calculateScoreTFIDF(double idf, int tf) {

        this.score += idf * (1 + Math.log10(tf));
    }

    public double getScore() {
        return score;
    }


}
