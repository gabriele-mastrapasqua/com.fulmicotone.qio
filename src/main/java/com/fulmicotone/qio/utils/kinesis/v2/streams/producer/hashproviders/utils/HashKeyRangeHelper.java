package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.utils;


import software.amazon.awssdk.services.kinesis.model.HashKeyRange;

import java.math.BigInteger;

public class HashKeyRangeHelper {


    public static boolean isRangeBetween(String range, HashKeyRange hashKeyRange){

        BigInteger rangeHash = new BigInteger(range);
        BigInteger lowerBound = new BigInteger(hashKeyRange.startingHashKey());
        BigInteger upperBound = new BigInteger(hashKeyRange.endingHashKey());

        return lowerBound.compareTo(rangeHash) < 0 && upperBound.compareTo(rangeHash) > 0;
    }
}
