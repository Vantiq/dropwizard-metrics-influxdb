package com.izettle.metrics.influxdb.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.Meter;
import com.izettle.metrics.influxdb.InfluxDbReporter.TagExtractor;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class MeasurementMappingCacheTest {

    @Test
    public void shouldMapMetricToMeasurementName() throws Exception {
        final String metricName = "com.example.resources.RandomResource";
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*resources.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings, Collections.emptyMap());
        String name = cache.getMeasurementName(metricName);
        assertThat(name).isEqualTo("resources");
        assertThat(cache.getMeasurementTags(metricName)).isEmpty();
    }

    @Test
    public void shouldMapMetricToMeasurementNameWithTags() throws Exception {
        final String metricName = "com.example.resources.RandomResource";
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*\\.resources\\.(?<resourceName>.*)"));

        Map<String, Map<String, Optional<TagExtractor>>> measurementTags = new HashMap<>();
        Map<String, Optional<TagExtractor>> tagKeys = new HashMap<>();
        tagKeys.put("resourceName", Optional.empty());
        measurementTags.put("resources", tagKeys);

        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings, measurementTags);
        String name = cache.getMeasurementName(metricName);
        assertThat(name).isEqualTo("resources");
        Map<String, String> tags = cache.getMeasurementTags(metricName);
        assertThat(tags).containsEntry("resourceName", "RandomResource");
    }

    @Test
    public void shouldMapMetricToMeasurementNameWithTagExtractor() throws Exception {
        final String metricName = "com.example.resources.RandomResource";
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*\\.resources\\.(?<resourceNameAlias>.*)"));

        Map<String, Map<String, Optional<TagExtractor>>> measurementTags = new HashMap<>();
        Map<String, Optional<TagExtractor>> tagKeys = new HashMap<>();
        tagKeys.put("resourceName", Optional.of(matcher -> matcher.group("resourceNameAlias")));
        measurementTags.put("resources", tagKeys);

        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings, measurementTags);
        String name = cache.getMeasurementName(metricName);
        assertThat(name).isEqualTo("resources");
        Map<String, String> tags = cache.getMeasurementTags(metricName);
        assertThat(tags).containsEntry("resourceName", "RandomResource");
    }

    @Test
    public void shouldNotMapMetricToMeasurementName() throws Exception {
        final String metricName = "com.example.resources.RandomResource";
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*health.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings, Collections.emptyMap());
        String name = cache.getMeasurementName(metricName);
        assertThat(name).isEqualTo(metricName);
        assertThat(cache.getMeasurementTags(metricName)).isEmpty();
    }

    @Test
    public void shouldMatchMetricWithMapping() throws Exception {
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*resources.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings, Collections.emptyMap());
        boolean match = cache.getFilter().matches("com.example.resources.RandomResource", mock(Meter.class));
        assertThat(match).isTrue();
    }

    @Test
    public void shouldNotMatchMetricWithoutMapping() throws Exception {
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*health.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings, Collections.emptyMap());
        boolean match = cache.getFilter().matches("com.example.resources.RandomResource", mock(Meter.class));
        assertThat(match).isFalse();
    }
}