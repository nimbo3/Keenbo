package in.nimbo.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import spark.ResponseTransformer;

public class JsonTransformer implements ResponseTransformer {
    private ObjectMapper mapper;
    private ObjectWriter writer;

    public JsonTransformer(ObjectMapper mapper, ObjectWriter writer) {
        this.mapper = mapper;
        this.writer = writer;
    }

    @Override
    public String render(Object o) throws Exception {
        return writer.writeValueAsString(o);
    }
}
