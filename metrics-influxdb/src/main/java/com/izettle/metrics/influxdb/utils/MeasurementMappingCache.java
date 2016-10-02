package com.izettle.metrics.influxdb.utils;

import com.codahale.metrics.MetricFilter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class MeasurementMappingCache {
    private final Map<String, Pattern> measurementMappings;

    private static class CacheEntry {
        final String measurementName;

        private CacheEntry(String measurementName) {
            this.measurementName = measurementName;
        }

        private boolean hasMapping() {
            return measurementName != null;
        }
    }
    private final Map<String, CacheEntry> cachedMappings;

    public MeasurementMappingCache(Map<String, Pattern> measurementMappings) {
        this.measurementMappings = measurementMappings != null ? measurementMappings : Collections.emptyMap();
        cachedMappings = new HashMap<>();
    }

    private CacheEntry getMapping(String metricName) {
        // Get entry from cache (creating if necessary)
        return cachedMappings.computeIfAbsent(metricName, name -> {
            for (Map.Entry<String, Pattern> entry : measurementMappings.entrySet()) {
                final Pattern pattern = entry.getValue();

                if (pattern.matcher(name).matches()) {
                    return new CacheEntry(entry.getKey());
                }
            }
            return new CacheEntry(null);
        });

    }

    /**
     * Get the measurement name associated with the given metric name.
     *
     * @param metricName name of the metric
     * @return name to use for measurement
     */
    public String getMeasurementName(String metricName) {
        // Return cached name if the mapping exists, original name if not
        CacheEntry mapping = getMapping(metricName);
        return mapping.hasMapping() ? mapping.measurementName : metricName;
    }

    /**
     * @return metric filter which will match any metric with an associated measurement name mapping
     */
    public MetricFilter getFilter() {
        return ((name, metric) -> getMapping(name).hasMapping());
    }
}
