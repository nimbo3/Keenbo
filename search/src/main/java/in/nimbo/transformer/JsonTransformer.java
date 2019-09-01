package in.nimbo.transformer;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import spark.ResponseTransformer;

public class JsonTransformer implements ResponseTransformer {
    private ObjectWriter writer;

    public JsonTransformer(ObjectWriter writer) {
        this.writer = writer;
    }

    @Override
    public String render(Object o) throws Exception {
        return writer.writeValueAsString(o);
    }
}
