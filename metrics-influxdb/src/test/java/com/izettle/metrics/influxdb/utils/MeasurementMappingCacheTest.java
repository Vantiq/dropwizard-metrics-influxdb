package com.izettle.metrics.influxdb.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.Meter;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class MeasurementMappingCacheTest {

    @Test
    public void shouldMapMetricToMeasurementName() throws Exception {
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*resources.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings);
        String name = cache.getMeasurementName("com.example.resources.RandomResource");
        assertThat(name).isEqualTo("resources");
    }

    @Test
    public void shouldNotMapMetricToMeasurementName() throws Exception {
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*health.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings);
        String name = cache.getMeasurementName("com.example.resources.RandomResource");
        assertThat(name).isEqualTo("com.example.resources.RandomResource");
    }

    @Test
    public void shouldMatchMetricWithMapping() throws Exception {
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*resources.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings);
        boolean match = cache.getFilter().matches("com.example.resources.RandomResource", mock(Meter.class));
        assertThat(match).isTrue();
    }

    @Test
    public void shouldNotMatchMetricWithoutMapping() throws Exception {
        Map<String, Pattern> measurementMappings = new HashMap<>();
        measurementMappings.put("resources", Pattern.compile(".*health.*"));
        MeasurementMappingCache cache = new MeasurementMappingCache(measurementMappings);
        boolean match = cache.getFilter().matches("com.example.resources.RandomResource", mock(Meter.class));
        assertThat(match).isFalse();
    }
}