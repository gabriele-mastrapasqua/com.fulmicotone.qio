package com.fulmicotone.qio.utils.kinesis.streams.hashproviders;


import com.amazonaws.services.kinesis.model.HashKeyRange;

import java.util.SplittableRandom;
import java.util.stream.Collectors;

public class KCLShardKeyHelper {


    private static final String CHARS = "0123456789";
    private static final int LESS_SIGNIFICANT_LENGTH = 20;
    private SplittableRandom random = new SplittableRandom();
    private String shardId;
    private Long upperMostSignificant;
    private Long lowerMostSignificant;
    private HashKeyRange range;


    public KCLShardKeyHelper(String shardId, HashKeyRange range)
    {
        this.shardId = shardId;
        this.range = range;

        int upperSubstring = range.getEndingHashKey().length()-LESS_SIGNIFICANT_LENGTH;
        int lowerSubstring = range.getStartingHashKey().length()-LESS_SIGNIFICANT_LENGTH;

        this.upperMostSignificant = Long.valueOf(range.getEndingHashKey().substring(0, upperSubstring))+1;
        this.lowerMostSignificant = range.getStartingHashKey().equals("0") ? 0L : Long.valueOf(range.getStartingHashKey().substring(0, lowerSubstring))-1;
    }



    public String generateRandomKey()
    {
        String mostSignificant = String.valueOf(random.nextLong(lowerMostSignificant, upperMostSignificant));

        String randomNumber = random.ints(LESS_SIGNIFICANT_LENGTH, 0, CHARS.length()).mapToObj(i -> "" + CHARS.charAt(i))
                .collect(Collectors.joining());

        return mostSignificant + randomNumber;
    }

    public String getShardId() {
        return shardId;
    }
}
