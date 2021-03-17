package com.engati.data.analytics.engine.druid.response;

import com.engati.data.analytics.engine.druid.response.impl.DruidResponseParserImp;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class DruidResponseParserImplTest {

  @InjectMocks
  private DruidResponseParserImp druidResponseParser;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test_convertTimeSeriesJson_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output =
        "[{\"timestamp\":\"2021-01-01T00:00:00.000Z\",\"result\":{\"new_users\":6785}}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, Object> result = new HashMap<>();
    result.put("new_users", 6785);

    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 1);
    Assert.assertEquals(actualResponse.keySet().iterator().next(),
        "2021-01-01T00:00:00.000Z");
    Assert.assertEquals(actualResponse.values().iterator().next().size(), 1);
    Assert.assertEquals(actualResponse.values().iterator().next().get(0), result);
  }

  @Test
  public void test_convertTimeSeriesJson_success_multiMetric() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"timestamp\":\"2021-01-01T00:00:00.000Z\","
        + "\"result\":{\"old_users\":1360.0,\"visitor_count\":8145,\"new_users\":6785}}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, Object> result = new HashMap<>();
    result.put("old_users", 1360.0);
    result.put("visitor_count", 8145);
    result.put("new_users", 6785);

    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 1);
    Assert.assertEquals(actualResponse.keySet().iterator().next(),
        "2021-01-01T00:00:00.000Z");
    Assert.assertEquals(actualResponse.values().iterator().next().size(), 1);
    Assert.assertEquals(actualResponse.values().iterator().next().get(0), result);
  }

  @Test
  public void test_convertTimeSeriesJson_success_empty() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertTrue(MapUtils.isEmpty(actualResponse));
  }

  @Test
  public void test_convertTimeSeriesJson_success_breakdownByTime() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"timestamp\":\"2020-01-01T00:00:00.000Z\","
        + "\"result\":{\"session_count\":35980}},"
        + "{\"timestamp\":\"2021-01-01T00:00:00.000Z\","
        + "\"result\":{\"session_count\":8918}}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, List<Map<String, Object>>> expectedResponse = new HashMap<>();
    Map<String, Object> firstResult = new HashMap<>();
    firstResult.put("session_count", 35980);
    expectedResponse.put("2020-01-01T00:00:00.000Z", Collections.singletonList(firstResult));
    Map<String, Object> secondResult = new HashMap<>();
    secondResult.put("session_count", 8918);
    expectedResponse.put("2021-01-01T00:00:00.000Z", Collections.singletonList(secondResult));
    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 2);
    Assert.assertEquals(actualResponse.keySet(), expectedResponse.keySet());
    Assert.assertEquals(actualResponse, expectedResponse);
  }

  @Test
  public void test_convertTopNJson_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"timestamp\":\"2020-04-05T00:00:00.000Z\","
        + "\"result\":[{\"city\":\"Bengaluru\",\"session_count\":4943}]}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, Object> result = new HashMap<>();
    result.put("city", "Bengaluru");
    result.put("session_count", 4943);
    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 1);
    Assert.assertEquals(actualResponse.keySet().iterator().next(),
        "2020-04-05T00:00:00.000Z");
    Assert.assertEquals(actualResponse.values().iterator().next().size(), 1);
    Assert.assertEquals(actualResponse.values().iterator().next().get(0), result);
  }

  @Test
  public void test_convertTopNJson_success_top3Results() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"timestamp\":\"2020-04-05T00:00:00.000Z\","
        + "\"result\":[{\"city\":\"Bengaluru\",\"session_count\":4943},"
        + "{\"city\":\"Mumbai\",\"session_count\":3622},"
        + "{\"city\":\"Pune\",\"session_count\":2983}]}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    List<Map<String, Object>> resultList = new ArrayList<>();
    Map<String, Object> result1 = new HashMap<>();
    result1.put("city", "Bengaluru");
    result1.put("session_count", 4943);
    resultList.add(result1);
    Map<String, Object> result2 = new HashMap<>();
    result2.put("city", "Mumbai");
    result2.put("session_count", 3622);
    resultList.add(result2);
    Map<String, Object> result3 = new HashMap<>();
    result3.put("city", "Pune");
    result3.put("session_count", 2983);
    resultList.add(result3);
    Map<String, List<Map<String, Object>>> expectedResponse = new HashMap<>();
    expectedResponse.put("2020-04-05T00:00:00.000Z", resultList);
    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 1);
    Assert.assertEquals(actualResponse.values().iterator().next().size(), 3);
    Assert.assertEquals(actualResponse, expectedResponse);
  }

  @Test
  public void test_convertTopNJson_success_empty() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"timestamp\":\"2020-04-05T00:00:00.000Z\","
        + "\"result\":[]}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 1);
    Assert.assertTrue(CollectionUtils.isEmpty(actualResponse.get("2020-04-05T00:00:00.000Z")));
  }

  @Test
  public void test_convertTopNJson_success_breakdownByTime() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"timestamp\":\"2020-01-01T00:00:00.000Z\","
        + "\"result\":[{\"city\":\"Bengaluru\",\"session_count\":4155}]},"
        + "{\"timestamp\":\"2021-01-01T00:00:00.000Z\","
        + "\"result\":[{\"city\":\"Bengaluru\",\"session_count\":788}]}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, List<Map<String, Object>>> expectedResponse = new HashMap<>();
    Map<String, Object> firstResult = new HashMap<>();
    firstResult.put("city", "Bengaluru");
    firstResult.put("session_count", 4155);
    expectedResponse.put("2020-01-01T00:00:00.000Z", Collections.singletonList(firstResult));
    Map<String, Object> secondResult = new HashMap<>();
    secondResult.put("city", "Bengaluru");
    secondResult.put("session_count", 788);
    expectedResponse.put("2021-01-01T00:00:00.000Z", Collections.singletonList(secondResult));
    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 2);
    Assert.assertEquals(actualResponse.keySet(), expectedResponse.keySet());
    Assert.assertEquals(actualResponse, expectedResponse);
  }

  @Test
  public void test_convertGroupByJson_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    String output = "[{\"version\":\"v1\",\"timestamp\":\"2020-01-01T00:00:00.000Z\","
        + "\"event\":{\"city\":\"(not set)\",\"session_count\":2247}}]";
    JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();
    Map<String, Object> result = new HashMap<>();
    result.put("city", "(not set)");
    result.put("session_count", 2247);

    Map<String, List<Map<String, Object>>> actualResponse = druidResponseParser
        .convertGroupByJsonToMap(queryResponse, botRef, customerId);
    Assert.assertEquals(actualResponse.keySet().size(), 1);
    Assert.assertEquals(actualResponse.keySet().iterator().next(),
        "2020-01-01T00:00:00.000Z");
    Assert.assertEquals(actualResponse.values().iterator().next().size(), 1);
    Assert.assertEquals(actualResponse.values().iterator().next().get(0), result);
  }
}


