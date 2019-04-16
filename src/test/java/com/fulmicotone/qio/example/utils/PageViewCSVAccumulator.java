package com.fulmicotone.qio.example.utils;

import com.fulmicotone.qio.components.accumulator.IQueueIOAccumulatorLengthFunction;
import com.fulmicotone.qio.components.accumulator.QueueIOAccumulator;
import com.fulmicotone.qio.example.models.PageView;

import java.util.List;

public class PageViewCSVAccumulator extends QueueIOAccumulator<PageView, PageView> {


    public PageViewCSVAccumulator(double byteSizeLimit) {
        super(byteSizeLimit);
    }

    @Override
    public List<PageView> getRecords() {
        return accumulator;
    }

    @Override
    public IQueueIOAccumulatorLengthFunction<PageView> accumulatorLengthFunction() {
        return pageView -> (double)(pageView.getUrl()+","+pageView.getUserId()).length();
    }
}
