package com.smartbuilding.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** Aggregated result for one zone and one window, published to the results topic as JSON. */
public class ZoneWindowResult {
    public String zoneId;
    public long windowStartMs;
    public long windowEndMs;
    public long events;
    public long readings;
    public int uniqueSensors;
    public Double avgTempC;
    public Double minTempC;
    public Double maxTempC;
    public Double avgOccupancy;
    public int maxOccupancy;
    public double energyKwh;
    public double avgPowerKw;
    public long hvacCommands;
    public int modeFlips;
    public Double avgResponseMs;
    public long maxResponseMs;
    public String controlStability;
    public long computedAtMs;

    public ZoneWindowResult() {
    }

    public static ZoneWindowResult from(String zoneId, long start, long end, ZoneAccumulator a, int unstableFlips) {
        ZoneWindowResult r = new ZoneWindowResult();
        r.zoneId = zoneId;
        r.windowStartMs = start;
        r.windowEndMs = end;
        r.events = a.events;
        r.readings = a.readings;
        r.uniqueSensors = a.sensors.size();
        r.avgTempC = a.tempCount > 0 ? round(a.tempSum / a.tempCount, 2) : null;
        r.minTempC = a.tempCount > 0 ? round(a.tempMin, 2) : null;
        r.maxTempC = a.tempCount > 0 ? round(a.tempMax, 2) : null;
        r.avgOccupancy = a.occCount > 0 ? round(a.occSum / a.occCount, 2) : null;
        r.maxOccupancy = a.occMax;
        r.energyKwh = round(a.energyKwh, 5);
        double hours = (end - start) / 3_600_000.0;
        r.avgPowerKw = hours > 0 ? round(a.energyKwh / hours, 3) : 0.0;
        r.hvacCommands = a.commands;
        r.modeFlips = countFlips(a.commandModes);
        r.avgResponseMs = a.commands > 0 ? round((double) a.responseSum / a.commands, 1) : null;
        r.maxResponseMs = a.responseMax;
        r.controlStability = r.modeFlips >= unstableFlips ? "OSCILLATING" : "STABLE";
        r.computedAtMs = System.currentTimeMillis();
        return r;
    }

    /** Number of times the HVAC mode changed between consecutive commands (ordered by event time). */
    static int countFlips(List<String> commandModes) {
        List<String> sorted = new ArrayList<>(commandModes);
        Collections.sort(sorted, (x, y) -> Long.compare(Long.parseLong(x.substring(0, x.indexOf(':'))),
                Long.parseLong(y.substring(0, y.indexOf(':')))));
        int flips = 0;
        String prev = null;
        for (String s : sorted) {
            String mode = s.substring(s.indexOf(':') + 1);
            if (prev != null && !prev.equals(mode)) {
                flips++;
            }
            prev = mode;
        }
        return flips;
    }

    private static double round(double v, int digits) {
        double f = Math.pow(10, digits);
        return Math.round(v * f) / f;
    }

    private static String num(Double v) {
        return v == null ? "null" : String.format(Locale.ROOT, "%s", v);
    }

    private static String str(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    public String toJson() {
        return "{"
                + "\"zone_id\":" + str(zoneId)
                + ",\"window_start_ms\":" + windowStartMs
                + ",\"window_end_ms\":" + windowEndMs
                + ",\"events\":" + events
                + ",\"readings\":" + readings
                + ",\"unique_sensors\":" + uniqueSensors
                + ",\"avg_temp_c\":" + num(avgTempC)
                + ",\"min_temp_c\":" + num(minTempC)
                + ",\"max_temp_c\":" + num(maxTempC)
                + ",\"avg_occupancy\":" + num(avgOccupancy)
                + ",\"max_occupancy\":" + maxOccupancy
                + ",\"energy_kwh\":" + num(energyKwh)
                + ",\"avg_power_kw\":" + num(avgPowerKw)
                + ",\"hvac_commands\":" + hvacCommands
                + ",\"mode_flips\":" + modeFlips
                + ",\"avg_response_ms\":" + num(avgResponseMs)
                + ",\"max_response_ms\":" + maxResponseMs
                + ",\"control_stability\":" + str(controlStability)
                + ",\"computed_at_ms\":" + computedAtMs
                + "}";
    }
}
