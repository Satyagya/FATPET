package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorType;
import com.engati.data.analytics.sdk.druid.interval.DruidTimeIntervalMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TimeSeriesQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TopNQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.LongSumAggregator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TopNQueryTest {

  @InjectMocks
  private TopNQuery topNQuery;

  @Mock
  private DruidQueryGenerator druidQueryGenerator;

  @Mock
  private DruidQueryExecutor druidQueryExecutor;

  @Mock
  private DruidResponseParser druidResponseParser;

  @Captor
  private ArgumentCaptor<String> stringCaptor;

  @Captor
  private ArgumentCaptor<JsonArray> jsonArrayCaptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test_timeSeriesQueryTest() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    DruidTimeIntervalMetaInfo timeIntervalMetaInfo = DruidTimeIntervalMetaInfo.builder()
        .startTime("2020-01-01T00:00:00.000Z").endTime("2021-01-01T00:00:00.000Z").build();
    DruidAggregatorMetaInfo druidAggregatorMetaInfo = DruidAggregatorMetaInfo.builder()
        .type(DruidAggregatorType.LONGSUM).aggregatorName("session_count")
        .fieldName("sessions").build();
    TopNQueryMetaInfo topNQueryMetaInfo = TopNQueryMetaInfo.builder()
        .dataSource("geo_traffic_5234_24075")
        .intervals(Collections.singletonList(timeIntervalMetaInfo))
        .grain("ALL")
        .druidAggregateMetaInfo(Collections.singletonList(druidAggregatorMetaInfo))
        .druidPostAggregateMetaInfo(Collections.emptyList())
        .druidFilterMetaInfo(null)
        .threshold(1)
        .metricValue("session_count")
        .metricType("top")
        .dimension("city").build();

    DruidAggregator sessionAggregator = new LongSumAggregator(
        druidAggregatorMetaInfo.getAggregatorName(), druidAggregatorMetaInfo.getFieldName());
    Mockito.when(druidQueryGenerator.generateAggregators(
        Collections.singletonList(druidAggregatorMetaInfo), botRef, customerId))
        .thenReturn(Collections.singletonList(sessionAggregator));
    Mockito.when(druidQueryGenerator.generatePostAggregator(Collections.emptyList(),
        botRef, customerId)).thenReturn(Collections.emptyList());
    Mockito.when(druidQueryGenerator.generateFilters(null,
        botRef, customerId)).thenReturn(null);

    String output = "[{\"timestamp\":\"2020-04-05T00:00:00.000Z\","
            + "\"result\":[{\"city\":\"Bengaluru\",\"session_count\":4943}]}]";
    JsonArray response = JsonParser.parseString(output).getAsJsonArray();
    Mockito.when(druidQueryExecutor.getResponseFromDruid(Mockito.anyString(), Mockito.eq(botRef),
        Mockito.eq(customerId))).thenReturn(response);

    Map<String, List<Map<String, Object>>> responseMap = new HashMap<>();
    Map<String, Object> result = new HashMap<>();
    result.put("session_count", 4943);
    result.put("city", "Bengaluru");
    responseMap.put("2021-01-01T00:00:00.000Z", Collections.singletonList(result));
    Mockito.when(druidResponseParser.convertJsonToMap(Mockito.any(), Mockito.eq(botRef),
        Mockito.eq(customerId))).thenReturn(responseMap);
    QueryResponse prevResponse = new QueryResponse();
    SimpleResponse simpleResponse = SimpleResponse.builder()
        .queryResponse(responseMap).build();
    simpleResponse.setType(ResponseType.SIMPLE.name());
    QueryResponse actualResponse = topNQuery.generateAndExecuteQuery(botRef, customerId,
        topNQueryMetaInfo, prevResponse);
    Assert.assertEquals(actualResponse, simpleResponse);
    Mockito.verify(druidQueryGenerator).generateAggregators(Collections
        .singletonList(druidAggregatorMetaInfo), botRef, customerId);
    Mockito.verify(druidQueryGenerator).generatePostAggregator(Collections
        .emptyList(), botRef, customerId);
    Mockito.verify(druidQueryGenerator).generateFilters(null,
        botRef, customerId);
    Mockito.verify(druidQueryExecutor).getResponseFromDruid(stringCaptor.capture(),
        Mockito.eq(botRef), Mockito.eq(customerId));
    Mockito.verify(druidResponseParser).convertJsonToMap(jsonArrayCaptor.capture(),
        Mockito.eq(botRef), Mockito.eq(customerId));
  }

  @After
  public void tearDown() {
    Mockito.verifyNoMoreInteractions(druidQueryGenerator, druidQueryExecutor, druidResponseParser);
  }
}
