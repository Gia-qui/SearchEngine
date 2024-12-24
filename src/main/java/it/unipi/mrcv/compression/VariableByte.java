package it.unipi.mrcv.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.log;

public class VariableByte {
    // this method receives an integer and returns a byte array with the variable byte encoding of the integer
    public static byte[] fromIntToVarByte(int input) { //the bit of control is the first one, its 1 if its the last byte, 0 otherwise
        byte[] ret;
        if (input == 0) {
            ret = new byte[]{0};
            return ret;
        }
        int n_bytes = (int) (log(input) / log(128)) + 1; // n of bytes necessary for encoding, cast to int and + 1 cause the cast "truncates" the decimal part
        ret = new byte[n_bytes];
        for (int byteEncoder = n_bytes - 1; byteEncoder >= 0; byteEncoder--) { // we start encoding from the last byte 7 bits at times
            ret[byteEncoder] = (byte) (input % 128 - 128); // all bytes before the last one are negative (starts with 1)
            input = input / 128;
        }
        ret[n_bytes - 1] += 128; //last byte is positive

        return ret;
    }

    // this method receives a integer array and returns a byte array with the variable byte encoding of the integers
    public static byte[] fromArrayIntToVarByte(ArrayList<Integer> input) {
        ByteBuffer buf = ByteBuffer.allocate(input.size() * (Integer.SIZE / Byte.SIZE));
        for (int number : input)
            buf.put(fromIntToVarByte(number));

        buf.flip();
        byte[] ret = new byte[buf.remaining()];
        buf.get(ret);

        return ret;
    }

    // this method receives a byte array with the variable byte encoding of the integer and returns an arraylist of integers
    public static ArrayList<Integer> fromByteToArrayInt(byte[] bytes) {
        ArrayList<Integer> numbers = new ArrayList<>();
        int n = 0;
        for (byte b : bytes) {
            if (b < 0) {
                n = 128 * n + (128 + b); //shift left of 1 byte and add the byte (the byte is negative so we add 128 to make it positive)
            } else {
                int num = (128 * n + b); //shift left of 1 byte and add the last byte
                numbers.add(num);
                n = 0;
            }
        }

        return numbers;
    }

}
