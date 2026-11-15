package com.smartbuilding.analytics;

/**
 * One telemetry event read from Kafka. Three event types share this POJO:
 * <ul>
 *   <li>{@code sensor_reading} (topic sensor-readings) - temperature / occupancy</li>
 *   <li>{@code energy_sample}  (topic energy-events)   - kWh consumed by a zone</li>
 *   <li>{@code hvac_command}   (topic energy-events)   - command applied by the HVAC controller</li>
 * </ul>
 * Public fields + no-arg constructor => Flink treats it as an efficient POJO type.
 */
public class TelemetryEvent {
    public String eventType;
    public String zoneId;
    public String sensorId;
    public long tsMs;
    public Double temperatureC;
    public Integer occupancyCount;
    public Double kwh;
    public String mode;
    public Long responseMs;

    public TelemetryEvent() {
    }

    @Override
    public String toString() {
        return eventType + "@" + zoneId + "@" + tsMs;
    }
}
