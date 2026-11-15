package com.smartbuilding.analytics;

import org.apache.flink.api.common.functions.AggregateFunction;

/** Incremental (stateful) aggregation of all events of one zone inside one window. */
public class ZoneWindowAggregator implements AggregateFunction<TelemetryEvent, ZoneAccumulator, ZoneAccumulator> {
    private static final long serialVersionUID = 1L;

    @Override
    public ZoneAccumulator createAccumulator() {
        return new ZoneAccumulator();
    }

    @Override
    public ZoneAccumulator add(TelemetryEvent e, ZoneAccumulator acc) {
        acc.events++;
        if ("sensor_reading".equals(e.eventType)) {
            acc.readings++;
            if (e.sensorId != null) {
                acc.sensors.add(e.sensorId);
            }
            if (e.temperatureC != null) {
                acc.tempSum += e.temperatureC;
                acc.tempCount++;
                acc.tempMin = Math.min(acc.tempMin, e.temperatureC);
                acc.tempMax = Math.max(acc.tempMax, e.temperatureC);
            }
            if (e.occupancyCount != null) {
                acc.occSum += e.occupancyCount;
                acc.occCount++;
                acc.occMax = Math.max(acc.occMax, e.occupancyCount);
            }
        } else if ("energy_sample".equals(e.eventType)) {
            if (e.kwh != null) {
                acc.energyKwh += e.kwh;
            }
        } else if ("hvac_command".equals(e.eventType)) {
            acc.commands++;
            if (e.responseMs != null) {
                acc.responseSum += e.responseMs;
                acc.responseMax = Math.max(acc.responseMax, e.responseMs);
            }
            if (e.mode != null) {
                acc.commandModes.add(e.tsMs + ":" + e.mode);
            }
        }
        return acc;
    }

    @Override
    public ZoneAccumulator getResult(ZoneAccumulator acc) {
        return acc;
    }

    @Override
    public ZoneAccumulator merge(ZoneAccumulator a, ZoneAccumulator b) {
        a.events += b.events;
        a.readings += b.readings;
        a.tempSum += b.tempSum;
        a.tempCount += b.tempCount;
        a.tempMin = Math.min(a.tempMin, b.tempMin);
        a.tempMax = Math.max(a.tempMax, b.tempMax);
        a.occSum += b.occSum;
        a.occCount += b.occCount;
        a.occMax = Math.max(a.occMax, b.occMax);
        a.energyKwh += b.energyKwh;
        a.commands += b.commands;
        a.responseSum += b.responseSum;
        a.responseMax = Math.max(a.responseMax, b.responseMax);
        a.sensors.addAll(b.sensors);
        a.commandModes.addAll(b.commandModes);
        return a;
    }
}
