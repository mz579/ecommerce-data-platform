package com.ecommerce.rt.config;

import java.io.Serializable;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Command-line configuration for the realtime daily-metrics job. */
public final class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public String bootstrapServers = "kafka:9092";
    public String sourceTopic = "ecommerce.events.v1";
    public String dlqTopic = "ecommerce.events.v1.dlq";
    public String groupId = "ecommerce-rt-flink-v1";
    public String startingOffsets = "earliest";
    public int parallelism = 2;
    public boolean sinkEnabled = true;

    public JobConfig() {}

    public static JobConfig parse(String[] args) {
        JobConfig config = new JobConfig();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--bootstrap":
                    config.bootstrapServers = value(args, ++i, arg);
                    break;
                case "--source-topic":
                    config.sourceTopic = value(args, ++i, arg);
                    break;
                case "--dlq-topic":
                    config.dlqTopic = value(args, ++i, arg);
                    break;
                case "--group-id":
                    config.groupId = value(args, ++i, arg);
                    break;
                case "--starting-offsets":
                    config.startingOffsets = value(args, ++i, arg);
                    break;
                case "--parallelism":
                    config.parallelism = Integer.parseInt(value(args, ++i, arg));
                    break;
                case "--no-sink":
                    config.sinkEnabled = false;
                    break;
                default:
                    throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }
        if (config.parallelism <= 0) {
            throw new IllegalArgumentException("--parallelism must be positive");
        }
        if (!config.startingOffsets.equals("earliest")
                && !config.startingOffsets.equals("latest")
                && !config.startingOffsets.equals("committed")) {
            throw new IllegalArgumentException(
                    "--starting-offsets must be earliest|latest|committed");
        }
        return config;
    }

    private static String value(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return args[index];
    }

    public OffsetsInitializer offsetsInitializer() {
        switch (startingOffsets) {
            case "latest":
                return OffsetsInitializer.latest();
            case "committed":
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case "earliest":
            default:
                return OffsetsInitializer.earliest();
        }
    }

    public static String usage() {
        return "Usage: ecommerce-rt-job [--bootstrap kafka:9092] "
                + "[--source-topic ecommerce.events.v1] [--dlq-topic ecommerce.events.v1.dlq] "
                + "[--group-id ecommerce-rt-flink-v1] "
                + "[--starting-offsets earliest|latest|committed] [--parallelism 2] [--no-sink]";
    }
}
