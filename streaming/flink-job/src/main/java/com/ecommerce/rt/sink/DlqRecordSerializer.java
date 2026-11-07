package com.ecommerce.rt.sink;

import com.ecommerce.rt.model.InvalidRecord;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Serializes invalid source records to the DLQ topic with the diagnostic headers required
 * by the design: {@code error_reason}, {@code source_topic}, {@code partition},
 * {@code offset} (plus {@code failed_at}). The original value bytes are preserved.
 */
public class DlqRecordSerializer implements KafkaRecordSerializationSchema<InvalidRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public DlqRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            InvalidRecord element, KafkaSinkContext context, Long timestamp) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("error_reason", bytes(element.reason));
        headers.add("source_topic", bytes(element.sourceTopic));
        headers.add("partition", bytes(Integer.toString(element.partition)));
        headers.add("offset", bytes(Long.toString(element.offset)));
        headers.add("failed_at", bytes(Instant.now().toString()));
        byte[] value = element.value == null ? new byte[0] : element.value;
        return new ProducerRecord<>(topic, null, timestamp, element.key, value, headers);
    }

    private static byte[] bytes(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
    }
}
