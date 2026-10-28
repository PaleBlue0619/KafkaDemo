import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

public class TestPartitioner implements Partitioner {
    public int partition(String s, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        if (new Random().nextBoolean() || new Random().nextBoolean())
            return 0;
        else if (new Random().nextBoolean() || new Random().nextBoolean())
            return 1;
        else if (new Random().nextBoolean() || new Random().nextBoolean())
            return 2;
        else return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}