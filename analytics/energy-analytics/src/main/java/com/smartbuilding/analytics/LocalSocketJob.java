package com.smartbuilding.analytics;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Local test entry point (no Kafka connector needed): reads newline-delimited JSON from a TCP
 * socket and prints each window result as a line starting with "RESULT ". The bridge script
 * local/flink_bridge.py connects this to Kafka. The production job is {@link EnergyAnalyticsJob};
 * both share {@link AnalyticsPipeline}.
 */
public class LocalSocketJob {
    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(p.getInt("parallelism", 2));
        AnalyticsPipeline.build(env.socketTextStream(p.get("host", "127.0.0.1"), p.getInt("port", 9999)),
                        p.getLong("window-seconds", 60), p.getLong("out-of-orderness-seconds", 5),
                        p.getInt("unstable-flips", 3))
                .addSink(new SinkFunction<ZoneWindowResult>() {
                    @Override
                    public void invoke(ZoneWindowResult value, Context context) {
                        System.out.println("RESULT " + value.toJson());
                        System.out.flush();
                    }
                }).name("stdout");
        env.execute("energy-analytics-local");
    }
}
