package com.fulmicotone.qio;

import com.fulmicotone.qio.models.OutputQueues;
import com.fulmicotone.qio.utils.functions.FnIsQIOInputOfQIO;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestUtilsFunctions {




    @Test
    public void testIsQIOInputOfQIO_NULL(){

        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, 1,
                100_000, null, t -> t);

        StringProducerQueueIO stringQueueIO2 = new StringProducerQueueIO(String.class, 1,
                100_000, null, t -> t);

        FnIsQIOInputOfQIO fnIsQIOInputOfQIO = new FnIsQIOInputOfQIO();

        boolean result = fnIsQIOInputOfQIO.apply(stringQueueIO, stringQueueIO2);

        Assert.assertTrue(!result);
    }

    @Test
    public void testIsQIOInputOfQIO_NOT_NULL_NOT_CONTAINS(){

        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, null), t -> t);

        StringProducerQueueIO stringQueueIO2 = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, null), t -> t);

        FnIsQIOInputOfQIO fnIsQIOInputOfQIO = new FnIsQIOInputOfQIO();

        boolean result = fnIsQIOInputOfQIO.apply(stringQueueIO, stringQueueIO2);

        Assert.assertTrue(!result);
    }

    @Test
    public void testIsQIOInputOfQIO_NOT_NULL_NOT_CONTAINS2(){

        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, null), t -> t);

        StringProducerQueueIO stringQueueIOc = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, stringQueueIO.getInputQueue()), t -> t);

        StringProducerQueueIO stringQueueIO2 = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, stringQueueIOc.getInputQueue()), t -> t);

        FnIsQIOInputOfQIO fnIsQIOInputOfQIO = new FnIsQIOInputOfQIO();

        boolean result = fnIsQIOInputOfQIO.apply(stringQueueIO, stringQueueIO2);

        Assert.assertTrue(!result);
    }

    @Test
    public void testIsQIOInputOfQIO_NOT_NULL_CONTAINS(){

        StringProducerQueueIO stringQueueIO = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, null), t -> t);

        StringProducerQueueIO stringQueueIO2 = new StringProducerQueueIO(String.class, 1,
                100_000, new OutputQueues().withQueue(String.class, stringQueueIO.getInputQueue()), t -> t);

        FnIsQIOInputOfQIO fnIsQIOInputOfQIO = new FnIsQIOInputOfQIO();

        boolean result = fnIsQIOInputOfQIO.apply(stringQueueIO, stringQueueIO2);

        Assert.assertTrue(result);
    }
}

