package com.fulmicotone.qio;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {


    protected static final long MEGABYTE = 1024L * 1024L;


    protected static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    protected List<String> stringsGenerator(int max)
    {
        List<String> arrayList = new ArrayList<>();
        for(int i = 0; i<max; i++){
            arrayList.add("String"+i);
        }
        return arrayList;
    }

    protected List<String> tenByteStrings(int max)
    {
        List<String> arrayList = new ArrayList<>();
        for(int i = 0; i<max; i++){
            arrayList.add("0123456789");
        }
        return arrayList;
    }
}
