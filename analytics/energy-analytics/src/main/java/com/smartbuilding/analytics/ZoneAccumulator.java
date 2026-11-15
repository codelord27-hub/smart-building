package com.smartbuilding.analytics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Window state kept per zone (managed and checkpointed by Flink). */
public class ZoneAccumulator {
    public long events;
    public long readings;
    public double tempSum;
    public long tempCount;
    public double tempMin = Double.MAX_VALUE;
    public double tempMax = -Double.MAX_VALUE;
    public double occSum;
    public long occCount;
    public int occMax;
    public double energyKwh;
    public long commands;
    public long responseSum;
    public long responseMax;
    public Set<String> sensors = new HashSet<>();
    /** HVAC commands as "tsMs:MODE" - sorted at window end to count mode flips. */
    public List<String> commandModes = new ArrayList<>();

    public ZoneAccumulator() {
    }
}
