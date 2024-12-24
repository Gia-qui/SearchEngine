package it.unipi.mrcv.data_structures;

// class to store the document number and the length of the document
public class DocIndexElem {
    private String documentNumber;
    private int docLength;

    public DocIndexElem(String documentNumber, int docLength) {
        this.documentNumber = documentNumber;
        this.docLength = docLength;
    }

    // getters and setters
    public String getDocumentNumber() {
        return documentNumber;
    }

    public void setDocumentNumber(String documentNumber) {
        this.documentNumber = documentNumber;
    }

    public int getDocLength() {
        return docLength;
    }

    public void setDocLength(int docLength) {
        this.docLength = docLength;
    }
}
