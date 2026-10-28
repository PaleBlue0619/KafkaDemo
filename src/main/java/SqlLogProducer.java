import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 最简单版：每秒往 Kafka 写 10 条记录。
 *
 * 一条 Kafka 消息就是一对 key-value，正好放你的两个字段：
 *     key   = SQL 语句（字符串）
 *     value = 耗时（毫秒，float）
 *
 */
public class SqlLogProducer {

    public static void main(String[] args) {
        // step1. 构造 Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");                // Kafka 地址
        props.put("key.serializer", StringSerializer.class.getName());   // key 是字符串
        props.put("value.serializer", FloatSerializer.class.getName());  // value 是 4 字节 float
        props.put("acks", "all");                                        // 等所有副本确认，不丢消息
        props.put("enable.idempotence", "true");                         // 重试不会写重
        props.put("batch.size", "5000");                                 // 每批写入的数据量
        props.put("max.block.ms", "5000");                               // broker 连不上时最多等 5 秒（默认 60 秒）
        props.put("partitioner.class","TestPartitioner");                // 自定义分区类

        var producer = new KafkaProducer<String, Float> (props);

        // Ctrl+C 时先 flush 再关闭，缓冲区里的消息不会丢
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        String topic = "sql-log-test";   // topic 名字，随便起，不存在会自动创建

        // 先把 topic 元数据拉下来：连不上 broker 就在这里报错，不用等发送超时
        try {
            producer.partitionsFor(topic);
        } catch (Exception e) {
            System.err.println("连不上 Kafka（localhost:9092）：" + e.getMessage());
            producer.close();
            return;
        }

        // step2. Kafka Producer对应逻辑: 每秒写 10 条
        int id = 0;
        while (true) {
            for (int i = 0; i < 30000; i++) {
                String sql = "SELECT * FROM user_info WHERE id = " + (id++);
                float costMs = (float) (Math.round((1.0 + Math.random() * 100) * 100) / 100.0);  // 模拟 1~101 ms，保留两位小数

                // 发送失败时把异常打出来
                try {
                    // 同步发送消息
                    ProducerRecord<String, Float> record = new ProducerRecord<>(topic, sql, costMs);
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println("消息发送成功，分区: " + metadata.partition() + ", 偏移量: " + metadata.offset());
                } catch (Exception e) {
                    System.err.println("消息发送失败: " + e.getMessage());
                }

//                producer.send(new ProducerRecord<String, Float>(topic, sql, costMs), (metadata, exception) -> {
//                    if (exception != null) {
//                        System.err.println("发送失败 key=" + sql + " : " + exception.getMessage());
//                    }
//                });

                System.out.printf("写入 key=%-45s value=%.2f ms%n", sql, costMs);
            }

            try {
                Thread.sleep(1000);   // 睡 1 秒，所以速度就是每秒 10 条
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
