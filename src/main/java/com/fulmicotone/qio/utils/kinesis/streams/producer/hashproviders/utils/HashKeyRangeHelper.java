package com.fulmicotone.qio.utils.kinesis.streams.producer.hashproviders.utils;

import com.amazonaws.services.kinesis.model.HashKeyRange;

import java.math.BigInteger;

public class HashKeyRangeHelper {


    public static boolean isRangeBetween(String range, HashKeyRange hashKeyRange){

        BigInteger rangeHash = new BigInteger(range);
        BigInteger lowerBound = new BigInteger(hashKeyRange.getStartingHashKey());
        BigInteger upperBound = new BigInteger(hashKeyRange.getEndingHashKey());

        return lowerBound.compareTo(rangeHash) < 0 && upperBound.compareTo(rangeHash) > 0;
    }
}
