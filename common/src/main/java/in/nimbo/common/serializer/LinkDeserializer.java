package in.nimbo.common.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.nimbo.common.entity.Link;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class LinkDeserializer implements Deserializer<Link> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Link deserialize(String topic, byte[] bytes) {
        return deserialize(topic, null, bytes);
    }

    @Override
    public Link deserialize(String topic, Headers headers, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(data, Link.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
