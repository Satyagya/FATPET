package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.GroupByQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

@Slf4j
@Component
public class ConversionRateMetric extends MetricHandler {

  private static final String METRIC_HANDLER_NAME = "conversion_rate";

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
    for (DruidQueryMetaInfo druidQuery: multiQueryMetaInfo.getMultiMetricQuery()) {
      QueryResponse response = new QueryResponse();
      responses.add(queryHandlerFactory.getQueryHandler(druidQueryMetaInfo.getQueryType().name())
          .generateAndExecuteQuery(botRef, customerId, druidQuery, response));
    }
    return getConversionRate(responses, multiQueryMetaInfo.getMetricName(),
        multiQueryMetaInfo.getModifier(), multiQueryMetaInfo.getLimit());
  }

  private QueryResponse getConversionRate(List<QueryResponse> responses, String metric,
      String modifier, Integer limit) {
    if (CollectionUtils.isNotEmpty(responses)) {
      if (ResponseType.GROUP_BY.equals(responses.get(0).getType())) {

      } else {

      }
    }
    return null;
  }

  private List<List<Map<String, Object>>> mergeTimeSeriesQuery(List<List<Map<String, Object>>>
      orderResponse, List<List<Map<String, Object>>> sessionResponse, String metric) {
    List<List<Map<String, Object>>> timeStampConversionRate = new ArrayList<>();
    List<Map<String, Object>> conversionRate = new ArrayList<>();
    for (int timeStampIndex = 0; timeStampIndex<orderResponse.size(); timeStampIndex++) {
      Map<String, Object> value = new HashMap<>();
      for (int resultIndex = 0; resultIndex<orderResponse.get(timeStampIndex).size(); resultIndex++) {
        Integer orderCount = (Integer) orderResponse.get(timeStampIndex).get(resultIndex)
            .get(Constants.ORDER_COUNT);
        Integer sessionCount = (Integer) sessionResponse.get(timeStampIndex).get(resultIndex)
            .get(Constants.SESSION_COUNT);
        Double cvr = calculateConversionRate(orderCount, sessionCount);
        value.put(metric, Constants.DECIMAL_FORMAT.format(cvr));
        conversionRate.add(value);
      }
      timeStampConversionRate.add(conversionRate);
    }
    return timeStampConversionRate;
  }

  private List<List<Map<String, String>>> mergeGroupByQuery(List<Map<String, Object>>
      orderResponse, List<Map<String, Object>> sessionResponse, String dimensionKey,
      String modifier, Integer limit) {
    List<Map<String, String>> geoCVRList = new ArrayList<>();
    Map<String, Integer> geoOrderCount = getGeoAndMetricMap(orderResponse,
        "order_count", dimensionKey);
    Map<String, Integer> geoSessionCount = getGeoAndMetricMap(sessionResponse,
        "sessions", dimensionKey);
    Set<String> geoSet = geoOrderCount.keySet();
    geoSet.retainAll(geoSessionCount.keySet());
    Map<String, Double> geoByValue = new HashMap<>();
    for (String geoValue: geoSet) {
      Double cvr = calculateConversionRate(geoOrderCount.get(geoValue),
          geoSessionCount.get(geoValue));
      geoByValue.put(geoValue, cvr);
    }
    geoByValue = applySortAndLimit(geoByValue, modifier, limit);
    for (String geoValue: geoByValue.keySet()) {
      Map<String, String> value = new HashMap<>();
      value.put(dimensionKey, geoValue);
      Double cvr = calculateConversionRate(geoOrderCount.get(geoValue),
          geoSessionCount.get(geoValue));
      value.put("conversion_rate",
          new DecimalFormat("#.##").format(cvr));
      geoCVRList.add(value);
    }
    return Collections.singletonList(geoCVRList);
  }

  private Map<String, Integer> getGeoAndMetricMap(List<Map<String, Object>> eventList,
      String metric, String geo) {
    return eventList.stream().collect(Collectors
        .toMap(key -> (String) key.get(geo), value -> (Integer) value.get(metric)));
  }

  private Map<String, Double> applySortAndLimit(Map<String, Double> geoByValue, String modifier,
      Integer limit) {
    if ("top".equals(modifier)) {
      return geoByValue.entrySet().stream().sorted(Collections.reverseOrder(comparingByValue()))
          .limit(limit).collect(Collectors
              .toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    } else {
      return geoByValue.entrySet().stream().sorted(comparingByValue())
          .limit(limit).collect(Collectors
              .toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }
  }

  private Double calculateConversionRate(int orders, int sessions) {
    if (orders > 0 && sessions > 0) {
      return ((double) orders / (double) sessions) * 100;
    }
    return 0D;
  }
}
