package com.smartbuilding.analytics;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Energy Analytics Service - Apache Flink job running on Dataproc (YARN).
 *
 * <p>Consumes sensor readings and energy/HVAC events from Kafka, performs a stateful
 * event-time tumbling-window aggregation per zone (default 1 minute) and publishes the
 * results to the separate {@code energy-analytics-results} Kafka topic.
 *
 * <pre>
 * flink run -m yarn-cluster energy-analytics.jar \
 *   --bootstrap 10.10.0.50:9094 --input-topics sensor-readings,energy-events \
 *   --output-topic energy-analytics-results --window-seconds 60
 * </pre>
 */
public class EnergyAnalyticsJob {

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String bootstrap = p.getRequired("bootstrap");
        List<String> inputTopics = Arrays.asList(p.get("input-topics", "sensor-readings,energy-events").split(","));
        String outputTopic = p.get("output-topic", "energy-analytics-results");
        String groupId = p.get("group-id", "energy-analytics-flink");
        long windowSeconds = p.getLong("window-seconds", 60);
        long outOfOrderness = p.getLong("out-of-orderness-seconds", 5);
        int unstableFlips = p.getInt("unstable-flips", 3);
        long checkpointMs = p.getLong("checkpoint-interval-ms", 30_000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        env.enableCheckpointing(checkpointMs, CheckpointingMode.AT_LEAST_ONCE);
        if (p.has("checkpoint-dir")) {
            env.getCheckpointConfig().setCheckpointStorage(p.get("checkpoint-dir"));
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "60000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();

        KafkaSink<ZoneWindowResult> sink = KafkaSink.<ZoneWindowResult>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.<ZoneWindowResult>builder()
                        .setTopic(outputTopic)
                        .setKeySerializationSchema(new ZoneKeySerializer())
                        .setValueSerializationSchema(new ResultJsonSerializer())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // timestamps/watermarks are assigned after parsing inside the pipeline
        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-telemetry")
                .uid("kafka-telemetry");

        AnalyticsPipeline.build(raw, windowSeconds, outOfOrderness, unstableFlips)
                .sinkTo(sink).name("kafka-results").uid("kafka-results");

        env.execute("energy-analytics (" + windowSeconds + "s tumbling windows per zone)");
    }

    /** Kafka record key = zone id (keeps all results of a zone in one partition, in order). */
    public static class ZoneKeySerializer implements SerializationSchema<ZoneWindowResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(ZoneWindowResult r) {
            return r.zoneId.getBytes(StandardCharsets.UTF_8);
        }
    }

    /** Kafka record value = JSON document of the window result. */
    public static class ResultJsonSerializer implements SerializationSchema<ZoneWindowResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(ZoneWindowResult r) {
            return r.toJson().getBytes(StandardCharsets.UTF_8);
        }
    }
}
