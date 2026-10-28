import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 最简单版：把 producer 写进 sql-log-test(topic) 的记录读出来打印。
 *
 * 读到的还是那对 key-value：
 *     key   = SQL 语句（字符串）
 *     value = 耗时（毫秒，float）
 *
 */
public class SqlLogConsumer {

    public static void main(String[] args) {
        // step1. 构造 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");                    // Kafka 地址
        props.put("group.id", "sql-log-monitor");                            // 消费者组，同组共用位点
        props.put("key.deserializer", StringDeserializer.class.getName());   // key 是字符串
        props.put("value.deserializer", FloatDeserializer.class.getName());  // value 是 4 字节 float

        // ---- 拉取：一次 fetch 攒多少、等多久 ----
        props.put("fetch.min.bytes", 1024);            // broker 至少攒 1KB 才返回；数据不够就先不回
        props.put("fetch.max.wait.ms", 500);           // 但最多等 500ms，两个条件谁先满足按谁算
        props.put("max.partition.fetch.bytes", 1048576); // 单次 fetch 每个分区最多拉 1MB（默认值）

        // ---- 会话：多久没心跳算掉线 ----
        props.put("session.timeout.ms", 10000);        // 10 秒没心跳就认定消费者死亡、触发再均衡
        // 注意：不能小于 broker 的 group.min.session.timeout.ms（默认 6 秒），否则启动报错

        // ---- 位点：从哪开始读、怎么提交 ----
        props.put("auto.offset.reset", "earliest");    // 没有位点/位点过期时从头读（earliest）/ 从最新读（latest）
        props.put("enable.auto.commit", "true");       // 自动提交位点
        props.put("auto.commit.interval.ms", 1000);    // 每 1 秒提交一次

        // ---- 分配与吞吐 ----
        // Range：把连续的分区整段分给消费者；RoundRobin：所有分区逐个轮着分
        // 分区数 >= 消费者数时两者效果差不多，多消费者时 RoundRobin 分得更均匀
        props.put("partition.assignment.strategy", RangeAssignor.class.getName());
        props.put("max.poll.records", 500);            // 单次 poll() 最多返回 500 条

        KafkaConsumer<String, Float> consumer = new KafkaConsumer<>(props);

        // Ctrl+C 时正常关闭：会触发再均衡并提交最后一次位点
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

        String topic = "sql-log-test";
        consumer.subscribe(Collections.singletonList(topic));

        // 2. Kafka Consumer对应逻辑:
        long count = 0; // 统计batch大小
        while (true) {
            // 没.fetch.min.bytes 没攒够时最多阻塞 500ms（fetch.max.wait.ms）
            ConsumerRecords<String, Float> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, Float> record : records) {
//                System.out.printf("收到 partition=%d offset=%-6d key=%-45s value=%.2f ms%n",
//                        record.partition(), record.offset(), record.key(), record.value());
                count++;
            }

            if (!records.isEmpty()) {
                System.out.printf("--- 本轮 %d 条，累计 %d 条 ---%n", records.count(), count);
            }
        }
    }
}
