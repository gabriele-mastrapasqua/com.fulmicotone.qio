package com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders;


import com.fulmicotone.qio.utils.kinesis.v2.streams.producer.hashproviders.interfaces.IExplicitShardKeyHelper;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;

import java.util.SplittableRandom;
import java.util.stream.Collectors;

public class ExplicitShardKeyHelper implements IExplicitShardKeyHelper {


    private static final String CHARS = "0123456789";
    private static final int LESS_SIGNIFICANT_LENGTH = 20;
    private SplittableRandom random = new SplittableRandom();
    private String shardId;
    private Long upperMostSignificant;
    private Long lowerMostSignificant;
    private HashKeyRange range;


    public ExplicitShardKeyHelper(String shardId, HashKeyRange range)
    {
        this.shardId = shardId;
        this.range = range;

        int upperSubstring = range.endingHashKey().length()-LESS_SIGNIFICANT_LENGTH;
        int lowerSubstring = range.startingHashKey().length()-LESS_SIGNIFICANT_LENGTH;

        this.upperMostSignificant = Long.valueOf(range.endingHashKey().substring(0, upperSubstring))+1;
        this.lowerMostSignificant = range.startingHashKey().equals("0") ? 0L : Long.valueOf(range.startingHashKey().substring(0, lowerSubstring))-1;
    }

    public String getShardId() {
        return shardId;
    }

    @Override
    public String generateHashKey() {
        String mostSignificant = String.valueOf(random.nextLong(lowerMostSignificant, upperMostSignificant));

        String randomNumber = random.ints(LESS_SIGNIFICANT_LENGTH, 0, CHARS.length()).mapToObj(i -> "" + CHARS.charAt(i))
                .collect(Collectors.joining());

        return mostSignificant + randomNumber;
    }
}
