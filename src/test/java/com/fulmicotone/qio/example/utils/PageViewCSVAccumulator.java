package com.fulmicotone.qio.example.utils;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;
import com.fulmicotone.qio.example.models.PageView;

public class PageViewCSVAccumulator extends QueueIOAccumulator<PageView> {


    public PageViewCSVAccumulator(double byteSizeLimit) {
        super(byteSizeLimit);
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<PageView> accumulatorLengthFunction() {
        return pageView -> (pageView.getUrl()+","+pageView.getUserId()).length();
    }
}
