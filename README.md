# QueueIOService - Java queue based microservice for heavy loaded system
&nbsp;[![Build Status](https://travis-ci.org/fulmicotone/com.fulmicotone.qio.svg?branch=master)](https://travis-ci.org/fulmicotone/com.fulmicotone.qio) &nbsp;[![](https://jitpack.io/v/fulmicotone/com.fulmicotone.qio.svg)](https://jitpack.io/#fulmicotone/com.fulmicotone.qio)


### Goal - Build a multi-threaded CPU intensive microservice system that...

- Makes simple handling differents data flows.
- Expose one simple interface easy to implement and thread-safe (because only one thread will execute the code)
- Microbatches elements based on count or size with periodic flushing timeout.
- Uses all the power of the machine/instance (with java FixedThreadPool based on machine cores)


### How is designed:

- The service expose a single input queue and could be intialized with one or more output queues.
- One single thread consumer read from input queue and send the input objects, in round-robin mode,  to N internal queues owned by N threads consumers.
- Every thread consumer listen on his own queue (Thread-safe) and consume/manipulate/transform objects applying a user-defined task.
- Every consumer could also produce output object/s and send them to output queues (typically to other QIOServies)

![alt tag](https://raw.githubusercontent.com/fulmicotone/com.fulmicotone.qio/develop/misc/qio.jpg)




### Installation:

Gradle, Maven etc.. from jitpack
https://jitpack.io/#fulmicotone/com.fulmicotone.qio



### Example

Imagine a pageview data stream coming from your website and you want to:
- Count every PageView request that comes from the same domain, sum them and printing result (just for fun).
- Transform one PageView object into a Intent object if some conditions occurs on database.
- Pass Intent objects to another stream that will save them on database of fired intents.

#### Example - services overview

![alt tag](https://raw.githubusercontent.com/fulmicotone/com.fulmicotone.qio/develop/misc/qio_example.jpg)

#### Example - services implementation
```java
// Example of one QIOService that will not produce anything.
public class IntentStorerQIO extends QueueIOService<Intent> {

    public IntentStorerQIO(Class<Intent> clazz, OutputQueues outputQueues) {
        super(clazz, outputQueues);
    }

    @Override
    // Developers should only implement the ingestionTask() method with service logic.
    public IQueueIOIngestionTask<Intent> ingestionTask() {
        return new IQueueIOIngestionTask<Intent>() {
            @Override
            public Void ingest(List<Intent> list) {
                list.forEach(this::saveToDB);
                return null;
            }

            private void saveToDB(Intent intent){
                // Call to DB HERE
            }
        };
    }
}


// Example of one QIOService that receive PageView objects and produce Intent objects
public class IntentDiscoverQIO extends QueueIOService<PageView> {

    public IntentDiscoverQIO(Class<PageView> clazz, OutputQueues outputQueues) {
        super(clazz, outputQueues);
    }

    @Override
    // Developers should only implement the ingestionTask() method with service logic.
    public IQueueIOIngestionTask<PageView> ingestionTask() {
        return new IQueueIOIngestionTask<PageView>() {

            // FAKE CALL to database - simulate both cases (found/not found) with random.
            private Optional<Intent> getIntentFromUserPageView(PageView userPageView)
            {
                // GET INTENT IF EXIST
            }

            @Override
            public Void ingest(List<PageView> list) {

                // Get Intent from database
                List<Intent> intents = list.stream()
                        .map(this::getIntentFromUserPageView)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());

                produceAll(intents, Intent.class);

                return null;
            }
        };
    }
}

```

#### Example: How to start QIOServices
```java
// THIS EXAMPLE CAN BE FOUND UNDER /test FOLDER (GithubExample.java)

// INTENT STORER QIO - Will store the Intent objects to the database.
IntentStorerQIO intentStorerQIO = new IntentStorerQIO(Intent.class, null);

// INTENT DISCOVER QIO - Output queue will be the IntentStorerQIO input queue
IntentDiscoverQIO intentDiscoverQIO = new IntentDiscoverQIO(PageView.class, new OutputQueues()
        .withQueue(Intent.class, intentStorerQIO.getInputQueue()));

// PAGEVIEW QIO - No output queues, just printing
DomainCountQIO domainCountQIO = new DomainCountQIO(PageView.class, null);

// START SERVICES
intentStorerQIO.startConsuming(2); // 2 THREADS
domainCountQIO.startConsuming(2); // 2 THREADS
intentDiscoverQIO.startConsuming(4); // 4 THREADS

```

#### Example - Batching

Batching example could be found into:
- GithubExampleByteAccumulator.java
- GithubExampleSizeAccumulator.java
