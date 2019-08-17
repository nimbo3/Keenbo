package in.nimbo.common.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import in.nimbo.common.entity.Page;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PageSerializer implements Serializer<Page> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Page page) {
        return serialize(topic, null, page);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Page page) {
        ObjectWriter writer = new ObjectMapper().writer();
        try {
            return writer.writeValueAsBytes(page);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }
}
