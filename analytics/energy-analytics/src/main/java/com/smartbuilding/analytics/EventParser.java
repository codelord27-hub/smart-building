package com.smartbuilding.analytics;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

/** Parses JSON Kafka records into {@link TelemetryEvent}; malformed records are counted and dropped. */
public class EventParser extends RichFlatMapFunction<String, TelemetryEvent> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;
    private transient Counter malformed;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
        malformed = getRuntimeContext().getMetricGroup().counter("malformedEvents");
    }

    @Override
    public void flatMap(String value, Collector<TelemetryEvent> out) {
        TelemetryEvent e = parse(mapper, value);
        if (e == null) {
            malformed.inc();
        } else {
            out.collect(e);
        }
    }

    /** Visible for tests. Returns null when the record cannot be used. */
    static TelemetryEvent parse(ObjectMapper mapper, String value) {
        try {
            JsonNode n = mapper.readTree(value);
            if (n == null || !n.isObject() || !n.hasNonNull("zone_id") || !n.hasNonNull("ts_ms")) {
                return null;
            }
            TelemetryEvent e = new TelemetryEvent();
            e.eventType = n.path("event_type").asText("sensor_reading");
            e.zoneId = n.get("zone_id").asText();
            e.tsMs = n.get("ts_ms").asLong();
            e.sensorId = n.hasNonNull("sensor_id") ? n.get("sensor_id").asText() : null;
            e.temperatureC = n.hasNonNull("temperature_c") ? n.get("temperature_c").asDouble() : null;
            e.occupancyCount = n.hasNonNull("occupancy_count") ? n.get("occupancy_count").asInt() : null;
            e.kwh = n.hasNonNull("kwh") ? n.get("kwh").asDouble() : null;
            e.mode = n.hasNonNull("mode") ? n.get("mode").asText() : null;
            e.responseMs = n.hasNonNull("response_ms") ? n.get("response_ms").asLong() : null;
            return e.tsMs > 0 && !e.zoneId.isEmpty() ? e : null;
        } catch (Exception ex) {
            return null;
        }
    }
}
