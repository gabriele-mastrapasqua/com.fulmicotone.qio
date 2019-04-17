package com.fulmicotone.qio.utils.kinesis.streams.hashproviders.utils;


import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.abstracts.AbstractStreamShardHelper;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IExplicitShardKeyHelper;
import com.fulmicotone.qio.utils.kinesis.streams.hashproviders.interfaces.IStreamShardHelper;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Iterators.cycle;

public class RoundRobinStreamShardHelper extends AbstractStreamShardHelper {

    private Iterator<IExplicitShardKeyHelper> iterator;


    @Override
    public IStreamShardHelper withHelpers(Set<IExplicitShardKeyHelper> helpers) {
        super.withHelpers(helpers);
        initIterator();
        return this;
    }

    private void initIterator(){
        iterator = cycle(helpers);
    }

    public IExplicitShardKeyHelper keyHelperForStream(){
        return iterator.next();
    }

}
