package com.ecommerce.rt.source;

import com.ecommerce.rt.model.RawRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

/**
 * Passes every Kafka record through unchanged, including malformed payloads.
 *
 * <p>Validation deliberately happens in {@code ParseFunction} instead of the
 * deserializer: a throwing deserializer would fail the source task and restart the whole
 * job on one bad message.
 */
public class RawKafkaDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) {
        out.collect(
                new RawRecord(
                        record.value(),
                        record.key(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.timestamp()));
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() {
        return TypeInformation.of(RawRecord.class);
    }
}
