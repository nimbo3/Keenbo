package in.nimbo.common.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import in.nimbo.common.entity.Link;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class LinkSerializer implements Serializer<Link> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Link link) {
        return serialize(topic, null, link);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Link link) {
        ObjectWriter writer = new ObjectMapper().writer();
        try {
            return writer.writeValueAsBytes(link);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }
}
