package in.nimbo.common.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import in.nimbo.common.entity.Page;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

;

public class PageDeserializer implements Deserializer<Page> { // TODO
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Page deserialize(String topic, byte[] bytes) {
        return deserialize(topic, null, bytes);
    }

    @Override
    public Page deserialize(String topic, Headers headers, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(Bytes.toString(data), Page.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
