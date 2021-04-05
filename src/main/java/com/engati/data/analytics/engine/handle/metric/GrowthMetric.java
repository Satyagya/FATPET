package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.interval.DruidTimeIntervalMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TimeSeriesQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.join.JoinTimeSeriesMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Months;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class GrowthMetric extends MetricHandler {

  private static final String METRIC_HANDLER_NAME = "growth_metric";

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;


  @Override
  public String getMetricName() {
    return METRIC_HANDLER_NAME;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    List<QueryResponse> responses = new ArrayList<>();
    MultiQueryMetaInfo multiQueryMetaInfo = ((MultiQueryMetaInfo) druidQueryMetaInfo);
    String grain = null;
    List<String> timeRange = Collections.emptyList();
    for (DruidQueryMetaInfo druidQuery : multiQueryMetaInfo.getMultiMetricQuery()) {
      if (druidQuery instanceof TimeSeriesQueryMetaInfo) {
        TimeSeriesQueryMetaInfo timeSeriesQueryMetaInfo = (TimeSeriesQueryMetaInfo) druidQuery;
        grain = timeSeriesQueryMetaInfo.getGrain();
        timeRange = getTimeRange(
            getPreviousStartTime(timeSeriesQueryMetaInfo.getIntervals().get(0).getStartTime(),
                grain), timeSeriesQueryMetaInfo.getIntervals().get(0).getEndTime(), grain);
        List<DruidTimeIntervalMetaInfo> druidTimeIntervalMetaInfos = Collections.singletonList(
            DruidTimeIntervalMetaInfo.builder().startTime(timeRange.get(0))
                .endTime(timeRange.get(timeRange.size() - 1)).build());
        timeSeriesQueryMetaInfo.setIntervals(druidTimeIntervalMetaInfos);
      } else if (druidQuery instanceof JoinTimeSeriesMetaInfo) {
        JoinTimeSeriesMetaInfo joinTimeSeriesMetaInfo = (JoinTimeSeriesMetaInfo) druidQuery;
        grain = joinTimeSeriesMetaInfo.getGrain();
        timeRange = getTimeRange(
            getPreviousStartTime(joinTimeSeriesMetaInfo.getIntervals().get(0).getStartTime(),
                grain), joinTimeSeriesMetaInfo.getIntervals().get(0).getEndTime(), grain);
        List<DruidTimeIntervalMetaInfo> druidTimeIntervalMetaInfos = Collections.singletonList(
            DruidTimeIntervalMetaInfo.builder().startTime(timeRange.get(0))
                .endTime(timeRange.get(timeRange.size() - 1)).build());
        joinTimeSeriesMetaInfo.setIntervals(druidTimeIntervalMetaInfos);
      }
      QueryResponse response = new QueryResponse();
      responses.add(queryHandlerFactory.getQueryHandler(druidQuery.getType(), botRef, customerId)
          .generateAndExecuteQuery(botRef, customerId, druidQuery, response));
    }
    SimpleResponse simpleResponse = (SimpleResponse) responses.get(0);
    return computeGrowth(timeRange, simpleResponse.getQueryResponse(),
        multiQueryMetaInfo.getMetricList().get(0));
  }

  private QueryResponse computeGrowth(List<String> timeRange,
      Map<String, List<Map<String, Object>>> simpleResponse, String metricName) {
    Map<String, List<Map<String, Object>>> modifiedSimpleResponse = new LinkedHashMap<>();
    for (Integer index = 0; index < timeRange.size() - 1; index++) {
      Map<String, Object> growthMap = new HashMap<>();
      Pair<Object, Object> prevAndCurrMetric =
          getPrevAndCurrentMetric(timeRange.get(index), timeRange.get(index + 1), metricName,
              simpleResponse);
      if (StringUtils.isNotEmpty(prevAndCurrMetric.getFirst().toString()) && StringUtils
          .isNotEmpty(prevAndCurrMetric.getSecond().toString())) {
        Double growth = (Double.parseDouble(prevAndCurrMetric.getSecond().toString()) - Double
            .parseDouble(prevAndCurrMetric.getFirst().toString())) / Double
            .parseDouble(prevAndCurrMetric.getFirst().toString());
        growthMap.put(Constants.GROWTH_METRIC, growth * 100);
        modifiedSimpleResponse.put(timeRange.get(index), Collections.singletonList(growthMap));
      } else {
        growthMap.put(Constants.GROWTH_METRIC, Constants.NOT_APPLICABLE);
        modifiedSimpleResponse.put(timeRange.get(index), Collections.singletonList(growthMap));
      }
    }
    SimpleResponse resultantResponse =
        SimpleResponse.builder().queryResponse(modifiedSimpleResponse).build();
    resultantResponse.setType(ResponseType.SIMPLE.name());
    return resultantResponse;
  }

  private String getPreviousStartTime(String startTime, String grain) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(Constants.ISO_TIME_FORMAT);
    DateTime startDate = formatter.parseDateTime(startTime);
    switch (grain) {
      case Constants.YEAR:
        startDate = startDate.minusYears(1);
        break;
      case Constants.MONTH:
        startDate = startDate.minusMonths(1);
        break;
      case Constants.WEEK:
        startDate = startDate.minusWeeks(1);
        break;
      case Constants.DAY:
        startDate = startDate.minusDays(1);
        break;
      case Constants.QUARTER:
        startDate = startDate.minusMonths(3);
        break;
      default:
        log.error("No matching grain found for : {}", grain);
        throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return startDate.toString(DateTimeFormat.forPattern(Constants.ISO_TIME_FORMAT));
  }

  private List<String> getTimeRange(String startTime, String endTime, String grain) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(Constants.ISO_TIME_FORMAT);
    DateTime startDate = formatter.parseDateTime(startTime);
    DateTime endDate = formatter.parseDateTime(endTime);
    List<DateTime> timeRange = Collections.emptyList();
    switch (grain) {
      case Constants.YEAR:
        long numOfYears = Years.yearsBetween(startDate, endDate).getYears();
        timeRange = Stream.iterate(startDate, date -> date.plusYears(1)).limit(numOfYears).limit(13)
            .filter(date -> date.getMonthOfYear() == 1).collect(Collectors.toList());
        break;
      case Constants.WEEK:
        long numOfDaysForWeek = Days.daysBetween(startDate, endDate).getDays();
        timeRange =
            Stream.iterate(startDate, date -> date.plusDays(1)).limit(numOfDaysForWeek).limit(91)
                .filter(date -> date.getDayOfWeek() == 1).collect(Collectors.toList());
        break;
      case Constants.MONTH:
        long numOfMonths = Months.monthsBetween(startDate, endDate).getMonths();
        timeRange =
            Stream.iterate(startDate, date -> date.plusMonths(1)).limit(numOfMonths).limit(13)
                .filter(date -> date.getDayOfMonth() == 1).collect(Collectors.toList());
        break;
      case Constants.DAY:
        long numOfDays = Days.daysBetween(startDate, endDate).getDays();
        timeRange = Stream.iterate(startDate, date -> date.plusDays(1)).limit(numOfDays).limit(13)
            .collect(Collectors.toList());
        break;
      case Constants.QUARTER:
        long months = Months.monthsBetween(startDate, endDate).getMonths();
        timeRange = Stream.iterate(startDate, date -> date.plusMonths(1)).limit(months).limit(36)
            .filter(date -> date.getMonthOfYear() == 1 || date.getMonthOfYear() == 4
                || date.getMonthOfYear() == 7 || date.getMonthOfYear() == 10)
            .collect(Collectors.toList());
        break;
      default:
        log.error("No matching grain found for : {}", grain);
        throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return timeRange.stream()
        .map(dateTime -> dateTime.toString(DateTimeFormat.forPattern(Constants.ISO_TIME_FORMAT)))
        .collect(Collectors.toList());
  }

  private Pair<Object, Object> getPrevAndCurrentMetric(String previous, String current,
      String metric, Map<String, List<Map<String, Object>>> simpleResponse) {
    Object currentMetric = StringUtils.EMPTY;
    Object previousMetric = StringUtils.EMPTY;
    if (Objects.nonNull(simpleResponse.getOrDefault(previous, null))) {
      previousMetric = simpleResponse.get(previous).get(0).getOrDefault(metric, StringUtils.EMPTY);
    }
    if (Objects.nonNull(simpleResponse.getOrDefault(current, null))) {
      currentMetric = simpleResponse.get(current).get(0).getOrDefault(metric, StringUtils.EMPTY);
    }
    return Pair.of(previousMetric, currentMetric);
  }
}