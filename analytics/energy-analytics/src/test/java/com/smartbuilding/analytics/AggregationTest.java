package com.smartbuilding.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class AggregationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    private TelemetryEvent ev(String json) {
        TelemetryEvent e = EventParser.parse(mapper, json);
        assertNotNull(e, json);
        return e;
    }

    @Test
    void parsesAllEventTypesAndRejectsGarbage() {
        TelemetryEvent r = ev("{\"event_type\":\"sensor_reading\",\"zone_id\":\"f0-reading-hall\",\"sensor_id\":\"s1\","
                + "\"temperature_c\":24.5,\"occupancy_count\":7,\"ts_ms\":1000}");
        assertEquals("f0-reading-hall", r.zoneId);
        assertEquals(24.5, r.temperatureC);
        assertEquals(7, r.occupancyCount);
        assertEquals("energy_sample", ev("{\"event_type\":\"energy_sample\",\"zone_id\":\"z1\",\"kwh\":0.02,\"ts_ms\":5}").eventType);
        assertNull(EventParser.parse(mapper, "not json"));
        assertNull(EventParser.parse(mapper, "{\"zone_id\":\"z1\"}"));
        assertNull(EventParser.parse(mapper, "[1,2,3]"));
    }

    @Test
    void aggregatesOneWindow() {
        ZoneWindowAggregator agg = new ZoneWindowAggregator();
        ZoneAccumulator acc = agg.createAccumulator();
        agg.add(ev("{\"zone_id\":\"z1\",\"sensor_id\":\"a\",\"temperature_c\":20,\"occupancy_count\":4,\"ts_ms\":1}"), acc);
        agg.add(ev("{\"zone_id\":\"z1\",\"sensor_id\":\"b\",\"temperature_c\":24,\"ts_ms\":2}"), acc);
        agg.add(ev("{\"zone_id\":\"z1\",\"sensor_id\":\"a\",\"temperature_c\":22,\"occupancy_count\":6,\"ts_ms\":3}"), acc);
        agg.add(ev("{\"event_type\":\"energy_sample\",\"zone_id\":\"z1\",\"kwh\":0.05,\"ts_ms\":4}"), acc);
        agg.add(ev("{\"event_type\":\"energy_sample\",\"zone_id\":\"z1\",\"kwh\":0.05,\"ts_ms\":5}"), acc);
        agg.add(ev("{\"event_type\":\"hvac_command\",\"zone_id\":\"z1\",\"mode\":\"COOL\",\"response_ms\":40,\"ts_ms\":6}"), acc);
        agg.add(ev("{\"event_type\":\"hvac_command\",\"zone_id\":\"z1\",\"mode\":\"HEAT\",\"response_ms\":60,\"ts_ms\":7}"), acc);

        ZoneWindowResult res = ZoneWindowResult.from("z1", 0, 60_000, acc, 3);
        assertEquals(7, res.events);
        assertEquals(3, res.readings);
        assertEquals(2, res.uniqueSensors);
        assertEquals(22.0, res.avgTempC);
        assertEquals(20.0, res.minTempC);
        assertEquals(24.0, res.maxTempC);
        assertEquals(5.0, res.avgOccupancy);
        assertEquals(0.1, res.energyKwh, 1e-9);
        assertEquals(6.0, res.avgPowerKw, 1e-9);   // 0.1 kWh in one minute = 6 kW
        assertEquals(2, res.hvacCommands);
        assertEquals(1, res.modeFlips);
        assertEquals(50.0, res.avgResponseMs);
        assertEquals("STABLE", res.controlStability);
        assertTrue(res.toJson().startsWith("{\"zone_id\":\"z1\""));
    }

    @Test
    void countsFlipsInEventTimeOrder() {
        // arrival order is shuffled - flips must be counted by timestamp
        assertEquals(4, ZoneWindowResult.countFlips(Arrays.asList("30:COOL", "10:COOL", "20:HEAT", "40:HEAT", "50:OFF")));
        assertEquals(0, ZoneWindowResult.countFlips(Arrays.asList("1:COOL", "2:COOL")));
    }

    @Test
    void mergeCombinesAccumulators() {
        ZoneWindowAggregator agg = new ZoneWindowAggregator();
        ZoneAccumulator a = agg.add(ev("{\"zone_id\":\"z\",\"sensor_id\":\"a\",\"temperature_c\":10,\"ts_ms\":1}"),
                agg.createAccumulator());
        ZoneAccumulator b = agg.add(ev("{\"zone_id\":\"z\",\"sensor_id\":\"b\",\"temperature_c\":30,\"ts_ms\":2}"),
                agg.createAccumulator());
        ZoneWindowResult res = ZoneWindowResult.from("z", 0, 60_000, agg.merge(a, b), 3);
        assertEquals(20.0, res.avgTempC);
        assertEquals(2, res.uniqueSensors);
        assertNull(res.avgOccupancy);
    }
}
