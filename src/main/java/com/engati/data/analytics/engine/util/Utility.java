package com.engati.data.analytics.engine.util;

import com.engati.data.analytics.engine.druid.query.druidry.metric.InvertedTopNMetric;
import com.engati.data.analytics.sdk.druid.interval.DruidTimeIntervalMetaInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.granularity.Granularity;
import in.zapr.druid.druidry.granularity.PredefinedGranularity;
import in.zapr.druid.druidry.granularity.SimpleGranularity;
import in.zapr.druid.druidry.query.DruidQuery;
import in.zapr.druid.druidry.topNMetric.SimpleMetric;
import in.zapr.druid.druidry.topNMetric.TopNMetric;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Utility {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  public static String convertDruidQueryToJsonString(DruidQuery druidQuery) {
    String requiredJson = null;
    try {
      requiredJson = MAPPER.writeValueAsString(druidQuery);
    } catch (JsonProcessingException ex) {
      log.error("jsonParsingException", ex);
    }
    return requiredJson;
  }

  public static String convertDataSource(Integer botRef, Integer customerId, String dataSource) {
    return String.format("%s_%s_%s", dataSource, customerId, botRef);
  }

  public static Granularity getGranularity(String grain) {
    return new SimpleGranularity(PredefinedGranularity.valueOf(grain));
  }

  public static List<Interval> extractInterval(List<DruidTimeIntervalMetaInfo>
      timeIntervalMetaInfoList) {
    List<Interval> intervals = new ArrayList<>();
    for (DruidTimeIntervalMetaInfo timeIntervalMetaInfo: timeIntervalMetaInfoList) {
      intervals.add(new Interval(DateTime.parse(timeIntervalMetaInfo.getStartTime()),
          DateTime.parse(timeIntervalMetaInfo.getEndTime())));
    }
    return intervals;
  }

  public static TopNMetric getMetric(String metricType, String metric) {
    if (Constants.TOP.equalsIgnoreCase(metricType)) {
      return new SimpleMetric(metric);
    } else {
      return new InvertedTopNMetric(metric);
    }
  }
}