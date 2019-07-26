package com.fulmicotone.qio.interfaces;


import com.fulmicotone.qio.services.QueueIOService;

import java.util.Collection;
import java.util.List;


public interface IQueueIOStatusServiceListener {


    void accept(Collection<QueueIOService> queueIOServiceList);

}
