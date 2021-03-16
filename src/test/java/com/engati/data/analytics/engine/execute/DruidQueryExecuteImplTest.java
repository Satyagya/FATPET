package com.engati.data.analytics.engine.execute;

import com.engati.data.analytics.engine.execute.impl.DruidQueryExecutorImpl;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import okhttp3.RequestBody;
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
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class DruidQueryExecuteImplTest {

  @InjectMocks
  private DruidQueryExecutorImpl druidQueryExecutor;

  @Mock
  private DruidServiceRetrofit druidServiceRetrofit;

  @Captor
  private ArgumentCaptor<RequestBody> requestBodyCaptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test_execute_druidQuery() {
    try {
      String druidJsonQuery =
          "{\"dataSource\":\"geo_traffic_5234_24075\"," + "\"queryType\":\"timeseries\","
              + "\"intervals\":[\"2021-01-01T00:00:00.000Z/2022-01-01T00:00:00.000Z\"],"
              + "\"granularity\":\"all\",\"aggregations\":[{\"type\":\"longSum\","
              + "\"name\":\"new_users\",\"fieldName\":\"new_users\"}],"
              + "\"postAggregations\":[]}";
      Integer botRef = 24075;
      Integer customerId = 5234;
      String output =
          "[{\"timestamp\":\"2021-01-01T00:00:00.000Z\",\"result\":{\"new_users\":6785}}]";
      JsonArray queryResponse = JsonParser.parseString(output).getAsJsonArray();

      Call druidMockedCall = Mockito.mock(Call.class);
      Mockito.when(druidServiceRetrofit.getResponseFromDruid(Mockito.any()))
          .thenReturn(druidMockedCall);
      Response<JsonArray> druidResponse = Response.success(queryResponse);
      Mockito.when(druidMockedCall.execute()).thenReturn(druidResponse);
      JsonArray actualResponse = druidQueryExecutor.getResponseFromDruid(druidJsonQuery, botRef,
          customerId);
      Assert.assertEquals(queryResponse, actualResponse);
      Mockito.verify(druidServiceRetrofit).getResponseFromDruid(requestBodyCaptor.capture());
    } catch (IOException ex) {
      Assert.fail("Exception from the retrofit call" + ex);
    }
  }

  @After
  public void tearDown() {
    Mockito.verifyNoMoreInteractions(druidServiceRetrofit);
  }
}
