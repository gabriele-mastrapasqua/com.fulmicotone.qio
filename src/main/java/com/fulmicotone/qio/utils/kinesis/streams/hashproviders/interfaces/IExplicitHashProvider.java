package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces;

public interface IExplicitHashProvider {

    String nextHashKeyForStream(String streamName);
}
