package it.unipi.mrcv.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

public class Unary {
    // this method receives an arraylist of integers and returns a byte array with the unary encoding of the integers
    public static byte[] ArrayIntToUnary(ArrayList<Integer> values) {
        /*
        ArrayList as an input (ex. {1,2,1,3})
        return a byte array with 0's between numbers (ex. 10110101 11000000)
         */
        BitSet bitSet = new BitSet();
        int lengthUnary;
        int j = 0;
        for (int i = 0; i < values.size(); i++) {
            lengthUnary = values.get(i);
            bitSet.set(j, j + lengthUnary);
            j = j + lengthUnary + 1;
            bitSet.set(j, false);
            j++;
        }

        if (bitSet.length() % 8 == 0) {

            bitSet.set(j, j + 7, false);
        }

        return bitSet.toByteArray();
    }

    // this method receives a byte array with the unary encoding of the integers and returns an arraylist of integers
    public static ArrayList<Integer> unaryToArrayInt(byte[] bytes) {

        BitSet b = BitSet.valueOf(bytes);
        ArrayList<Integer> arrayInt = new ArrayList<>();

        int i_zero;
        int i_one = 0;

        for (; i_one >= 0; i_one = b.nextSetBit(i_zero)) {
            i_zero = b.nextClearBit(i_one);
            arrayInt.add(i_zero - i_one);
        }
        return arrayInt;
    }

    // this method receives an integer and returns a byte array with the unary encoding of the integer
    public static byte[] intToUnary(int input) {
        BitSet bitSet = new BitSet();
        bitSet.set(0, input, true);
        return bitSet.toByteArray();
    }


}