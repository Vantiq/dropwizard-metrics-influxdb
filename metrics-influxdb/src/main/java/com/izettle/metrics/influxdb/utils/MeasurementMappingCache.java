package com.izettle.metrics.influxdb.utils;

import com.codahale.metrics.MetricFilter;
import com.izettle.metrics.influxdb.InfluxDbReporter.TagExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MeasurementMappingCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementMappingCache.class);

    private final Map<String, Pattern> measurementMappings;
    private final Map<String, Map<String, Optional<TagExtractor>>> tagExtractors;

    private static class CacheEntry {
        final String measurementName;
        final Map<String, String> tags;

        private CacheEntry(String measurementName, Map<String, String> tags) {
            this.measurementName = measurementName;
            this.tags = tags;
        }

        private boolean hasMapping() {
            return measurementName != null;
        }
    }
    private final Map<String, CacheEntry> cachedMappings;

    public MeasurementMappingCache(Map<String, Pattern> measurementMappings,
                                   Map<String, Map<String, Optional<TagExtractor>>> tagExtractors) {
        this.measurementMappings = measurementMappings != null ? measurementMappings : Collections.emptyMap();
        this.tagExtractors = tagExtractors != null ? tagExtractors : Collections.emptyMap();
        cachedMappings = new HashMap<>();
    }

    private CacheEntry getMapping(String metricName) {
        // Get entry from cache (creating if necessary)
        return cachedMappings.computeIfAbsent(metricName, name -> {
            for (Map.Entry<String, Pattern> entry : measurementMappings.entrySet()) {
                // Get pattern and check for a match
                final Pattern pattern = entry.getValue();
                Matcher matcher = pattern.matcher(name);
                if (matcher.matches()) {
                    // Get the name and then extract any tags that have been declared.
                    String measurementName = entry.getKey();
                    Map<String, String> tags = Collections.emptyMap();
                    if (tagExtractors.containsKey(measurementName)) {
                        Map<String, Optional<TagExtractor>> extractors = tagExtractors.get(measurementName);
                        Set<String> tagKeys = extractors.keySet();
                        tags = tagKeys.stream().collect(HashMap::new, (tagMap, tagKey) -> {
                            try {
                                TagExtractor extractor = extractors.get(tagKey).orElse(defaultExtractor(tagKey));
                                tagMap.put(tagKey, extractor.apply(matcher));
                            } catch (Exception e) {
                                LOGGER.warn("Failed to extract tag {} for metric {}.", metricName, tagKey, e);
                            }
                        }, Map::putAll);
                    }
                    return new CacheEntry(measurementName, tags);
                }
            }
            return new CacheEntry(null, Collections.emptyMap());
        });
    }

    private static TagExtractor defaultExtractor(String groupName) {
        return matcher -> matcher.group(groupName);
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
     * Get the tags associated with the given metric name.
     *
     * @param metricName name of the metric
     * @return map of tags where key is the tag key and value is the tag value
     */
    public Map<String, String> getMeasurementTags(String metricName) {
        return getMapping(metricName).tags;
    }

    /**
     * @return metric filter which will match any metric with an associated measurement name mapping
     */
    public MetricFilter getFilter() {
        return ((name, metric) -> getMapping(name).hasMapping());
    }
}
