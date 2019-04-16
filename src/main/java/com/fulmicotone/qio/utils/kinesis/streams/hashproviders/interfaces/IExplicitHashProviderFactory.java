package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces;

public interface IExplicitHashProviderFactory<I extends IExplicitHashProvider> {

    I getHashProvider();
}
