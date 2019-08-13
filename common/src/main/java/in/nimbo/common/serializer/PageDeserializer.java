package in.nimbo.common.serializer;

import in.nimbo.common.entity.Page;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PageDeserializer implements Deserializer<Page> { // TODO
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Page deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public Page deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
