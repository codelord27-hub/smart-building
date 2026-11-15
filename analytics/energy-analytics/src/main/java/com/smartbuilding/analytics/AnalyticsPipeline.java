package com.smartbuilding.analytics;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Source-independent core of the Energy Analytics job:
 * JSON strings -> parse -> event-time watermarks -> keyBy(zone) -> tumbling window -> aggregate.
 */
public final class AnalyticsPipeline {
    private AnalyticsPipeline() {
    }

    public static DataStream<ZoneWindowResult> build(DataStream<String> json, long windowSeconds,
                                                     long outOfOrdernessSeconds, int unstableFlips) {
        WatermarkStrategy<TelemetryEvent> watermarks = WatermarkStrategy
                .<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSeconds))
                .withTimestampAssigner((event, recordTs) -> event.tsMs)
                .withIdleness(Duration.ofSeconds(30));

        return json
                .flatMap(new EventParser()).name("parse-json").uid("parse-json")
                .assignTimestampsAndWatermarks(watermarks).name("event-time").uid("event-time")
                .keyBy(e -> e.zoneId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .aggregate(new ZoneWindowAggregator(), new ToResult(unstableFlips))
                .name("zone-window-aggregate").uid("zone-window-aggregate");
    }

    /** Attaches window boundaries + key to the incrementally aggregated accumulator. */
    static class ToResult extends ProcessWindowFunction<ZoneAccumulator, ZoneWindowResult, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final int unstableFlips;

        ToResult(int unstableFlips) {
            this.unstableFlips = unstableFlips;
        }

        @Override
        public void process(String zoneId, Context ctx, Iterable<ZoneAccumulator> accs, Collector<ZoneWindowResult> out) {
            for (ZoneAccumulator acc : accs) {
                out.collect(ZoneWindowResult.from(zoneId, ctx.window().getStart(), ctx.window().getEnd(), acc,
                        unstableFlips));
            }
        }
    }
}
