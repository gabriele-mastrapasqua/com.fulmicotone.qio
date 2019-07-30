package com.fulmicotone.qio.v2.functions;

import com.fulmicotone.qio.utils.kinesis.v2.streams.common.interfaces.IKinesisDataTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FnKinesisJsonStringTransform  implements IKinesisDataTransform<String, String> {
    @Override
    public Optional<List<String>> apply(Optional<String> object) {
        ArrayList<String> list = new ArrayList<>();
        if(object.isPresent())
            list.add(object.get());
        return Optional.of(list);
    }
}
