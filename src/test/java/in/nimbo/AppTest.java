package in.nimbo;

import in.nimbo.conf.Config;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private HBaseDAO dao = new HBaseDAOImpl(HBaseConfiguration.create(), new Config());

    public AppTest() throws IOException {
    }

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws Exception {
        dao.add("a");
    }
}
