package in.nimbo.common.serializer;

import in.nimbo.common.entity.Page;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PageSerializer implements Serializer<Page> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Page page) {
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Page data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }// TODO
   }
