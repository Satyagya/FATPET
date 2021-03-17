package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.druidry.DruidScanQuery;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.QueryDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.TableDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.join.DruidJoin;
import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorType;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterType;
import com.engati.data.analytics.sdk.druid.interval.DruidTimeIntervalMetaInfo;
import com.engati.data.analytics.sdk.druid.query.ScanMetaInfo;
import com.engati.data.analytics.sdk.druid.query.datasource.DataSourceEnum;
import com.engati.data.analytics.sdk.druid.query.datasource.QueryDataSourceType;
import com.engati.data.analytics.sdk.druid.query.datasource.SimpleDataSourceType;
import com.engati.data.analytics.sdk.druid.query.join.JoinMetaInfo;
import com.engati.data.analytics.sdk.druid.query.join.JoinTimeSeriesMetaInfo;
import com.engati.data.analytics.sdk.druid.query.join.JoinTopNMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.JavaScriptAggregator;
import in.zapr.druid.druidry.filter.InFilter;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class JoinTopNQueryTest {

  @InjectMocks
  private JoinTopNQuery joinTopNQuery;

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
  public void test_joinTimeSeriesQueryTest() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    DruidTimeIntervalMetaInfo timeIntervalMetaInfo = DruidTimeIntervalMetaInfo.builder()
        .startTime("2020-01-01T00:00:00.000Z").endTime("2021-01-01T00:00:00.000Z").build();
    DruidAggregatorMetaInfo druidAggregatorMetaInfo = DruidAggregatorMetaInfo.builder()
        .type(DruidAggregatorType.JAVASCRIPT)
        .aggregatorName("sales")
        .fields(Arrays.asList("line_item_price", "line_item_quantity"))
        .fnAggregate("function(currentSalesSum, price, quantity) "
            + "{ return currentSalesSum + (price * quantity); }")
        .fnCombine("function(partialSales, salesRespectToSegment) "
            + "{ return partialSales + salesRespectToSegment; }")
        .fnReset("function() { return 0; }").build();

    SimpleDataSourceType simpleDataSource = SimpleDataSourceType.builder()
        .dataSource("order_5234_24075").build();
    simpleDataSource.setDataSource(DataSourceEnum.TABLE.getValue());
    ScanMetaInfo scanMetaInfo = ScanMetaInfo.builder().dataSource("geo_traffic_5234_24075")
        .intervals(Collections.singletonList(timeIntervalMetaInfo))
        .columns(Arrays.asList("source", "medium", "transaction_id")).build();
    QueryDataSourceType queryDataSource = QueryDataSourceType.builder()
        .druidQueryMetaInfo(scanMetaInfo).build();
    queryDataSource.setType(DataSourceEnum.QUERY.getValue());
    String condition = "name == \"r.transaction_id\"";
    String rightPrefix = "r.";
    String joinType = "INNER";
    JoinMetaInfo joinMetaInfo = JoinMetaInfo.builder()
        .leftDataSource(simpleDataSource).rightDataSource(queryDataSource)
        .joinCondition(condition)
        .rightPrefix(rightPrefix)
        .joinType(joinType).build();

    DruidFilterMetaInfo druidFilterMetaInfo = DruidFilterMetaInfo.builder()
        .value("google").type(DruidFilterType.IN)
        .dimension("source").build();

    JoinTopNMetaInfo joinTopNMetaInfo = JoinTopNMetaInfo.builder()
        .dataSource(joinMetaInfo)
        .intervals(Collections.singletonList(timeIntervalMetaInfo))
        .grain("ALL")
        .druidAggregateMetaInfo(Collections.singletonList(druidAggregatorMetaInfo))
        .druidPostAggregateMetaInfo(Collections.emptyList())
        .druidFilterMetaInfo(druidFilterMetaInfo)
        .threshold(1)
        .metricValue("sales")
        .metricType("top")
        .dimension("source")
        .build();

    DruidAggregator salesAggregator = JavaScriptAggregator.builder()
        .name(druidAggregatorMetaInfo.getAggregatorName())
        .fieldNames(druidAggregatorMetaInfo.getFields())
        .functionAggregate(druidAggregatorMetaInfo.getFnAggregate())
        .functionCombine(druidAggregatorMetaInfo.getFnCombine())
        .functionReset(druidAggregatorMetaInfo.getFnReset()).build();
    DruidJoin druidJoin = DruidJoin.builder().left(new TableDataSource("order_5234_24075"))
        .right(new QueryDataSource(DruidScanQuery.builder()
            .dataSource("geo_traffic_5234_24075")
            .columns(Arrays.asList("source", "medium", "transaction_id"))
            .intervals(Utility.extractInterval(Collections.singletonList(timeIntervalMetaInfo)))
            .build()))
        .rightPrefix(rightPrefix)
        .condition(condition)
        .joinType(joinType)
        .build();
    Mockito.when(druidQueryGenerator.getJoinDataSource(joinMetaInfo, botRef ,customerId))
        .thenReturn(druidJoin);
    Mockito.when(druidQueryGenerator.generateAggregators(
        Collections.singletonList(druidAggregatorMetaInfo), botRef, customerId))
        .thenReturn(Collections.singletonList(salesAggregator));
    Mockito.when(druidQueryGenerator.generatePostAggregator(Collections.emptyList(),
        botRef, customerId)).thenReturn(Collections.emptyList());
    Mockito.when(druidQueryGenerator.generateFilters(druidFilterMetaInfo,
        botRef, customerId)).thenReturn(new InFilter("source", Arrays.asList("google")));

    String output = "[{\"timestamp\":\"2020-04-05T00:00:00.000Z\","
        + "\"result\":[{\"source\":\"google\",\"sales\":4943}]}]";
    JsonArray response = JsonParser.parseString(output).getAsJsonArray();
    Mockito.when(druidQueryExecutor.getResponseFromDruid(Mockito.anyString(), Mockito.eq(botRef),
        Mockito.eq(customerId))).thenReturn(response);

    Map<String, List<Map<String, Object>>> responseMap = new HashMap<>();
    Map<String, Object> result = new HashMap<>();
    result.put("sales", 4943);
    result.put("source", "google");
    responseMap.put("2020-04-05T00:00:00.000Z", Collections.singletonList(result));
    Mockito.when(druidResponseParser.convertJsonToMap(Mockito.any(), Mockito.eq(botRef),
        Mockito.eq(customerId))).thenReturn(responseMap);
    QueryResponse prevResponse = new QueryResponse();
    SimpleResponse simpleResponse = SimpleResponse.builder()
        .queryResponse(responseMap).build();
    simpleResponse.setType(ResponseType.SIMPLE.name());
    QueryResponse actualResponse = joinTopNQuery.generateAndExecuteQuery(botRef, customerId,
        joinTopNMetaInfo, prevResponse);
    Assert.assertEquals(actualResponse, simpleResponse);
    Mockito.verify(druidQueryGenerator).getJoinDataSource(joinMetaInfo, botRef, customerId);
    Mockito.verify(druidQueryGenerator).generateAggregators(Collections
        .singletonList(druidAggregatorMetaInfo), botRef, customerId);
    Mockito.verify(druidQueryGenerator).generatePostAggregator(Collections
        .emptyList(), botRef, customerId);
    Mockito.verify(druidQueryGenerator).generateFilters(druidFilterMetaInfo,
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
