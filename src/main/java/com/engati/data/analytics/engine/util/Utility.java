package com.engati.data.analytics.engine.util;

import com.engati.data.analytics.engine.druid.query.druidry.ScanQuery;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.DruidDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.QueryDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.TableDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.join.DruidJoin;
import com.engati.data.analytics.engine.druid.query.druidry.metric.InvertedTopNMetric;
import com.engati.data.analytics.sdk.druid.interval.DruidTimeIntervalMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.ScanMetaInfo;
import com.engati.data.analytics.sdk.druid.query.datasource.DataSourceType;
import com.engati.data.analytics.sdk.druid.query.datasource.QueryDataSourceType;
import com.engati.data.analytics.sdk.druid.query.datasource.SimpleDataSourceType;
import com.engati.data.analytics.sdk.druid.query.join.JoinMetaInfo;
import com.engati.data.analytics.sdk.response.SimpleResponse;
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
import java.util.Objects;

@Slf4j
public class Utility {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  public static String convertDruidQueryToJsonString(Object druidQuery) {
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

  public static DruidJoin getDruidJoin(JoinMetaInfo joinMetaInfo, Integer botRef,
      Integer customerId) {
    DruidDataSource left = getDataSource(joinMetaInfo.getLeftDataSource(),
        botRef, customerId);
    DruidDataSource right = getDataSource(joinMetaInfo.getRightDataSource(),
        botRef, customerId);
    return DruidJoin.builder().left(left).right(right)
        .rightPrefix(joinMetaInfo.getRightPrefix())
        .condition(joinMetaInfo.getJoinCondition())
        .joinType(joinMetaInfo.getJoinType()).build();
  }

  private static DruidDataSource getDataSource(DataSourceType dataSource, Integer botRef,
      Integer customerId) {
    DruidDataSource druidDataSource = null;
    if (dataSource instanceof SimpleDataSourceType) {
      druidDataSource = new TableDataSource(((SimpleDataSourceType) dataSource)
          .getDataSource());
    } else if (dataSource instanceof QueryDataSourceType) {
      Object query = generateQueryFromMetaInfo(((QueryDataSourceType) dataSource)
          .getDruidQueryMetaInfo(), botRef, customerId);
      druidDataSource = new QueryDataSource(query);
    }
    return druidDataSource;
  }

  private static Object generateQueryFromMetaInfo(DruidQueryMetaInfo
      druidQueryMetaInfo, Integer botRef, Integer customerId) {
    Object query = null;
    if (druidQueryMetaInfo instanceof ScanMetaInfo) {
      query = getScanQuery(((ScanMetaInfo) druidQueryMetaInfo), botRef, customerId);
    }
    return query;
  }

  private static ScanQuery getScanQuery(ScanMetaInfo scanMetaInfo, Integer botRef,
      Integer customerId) {
    return ScanQuery.builder()
        .dataSource(convertDataSource(botRef, customerId, scanMetaInfo.getDataSource()))
        .intervals(extractInterval(scanMetaInfo.getIntervals()))
        .columns(scanMetaInfo.getColumns()).build();
  }

  public static SimpleResponse mergePreviousResponse(SimpleResponse response,
      SimpleResponse prevResponse) {
    if (Objects.isNull(prevResponse) || Objects.isNull(prevResponse.getQueryResponse())
        || prevResponse.getQueryResponse().isEmpty()) {
      return response;
    } else {
      for (int resultIndex = 0; resultIndex < response.getQueryResponse().size(); resultIndex++) {
        for (int index = 0; index < response.getQueryResponse().get(resultIndex).size(); index++) {
          prevResponse.getQueryResponse().get(resultIndex).get(index)
              .putAll(response.getQueryResponse().get(resultIndex).get(index));
        }
      }
    }
    return prevResponse;
  }
}
