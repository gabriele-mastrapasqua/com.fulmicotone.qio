package com.fulmicotone.qio.interfaces;


import java.util.List;

@FunctionalInterface
public interface IQueueIOIngestionTask<I> {

    Void ingest(List<I> list);
}
