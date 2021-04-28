package com.engati.data.analytics.ingestion;

import com.engati.data.analytics.engine.ingestion.impl.IngestionHandlerServiceImpl;
import com.engati.data.analytics.engine.retrofit.DruidIngestionServiceRetrofit;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.google.gson.JsonObject;
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
import org.springframework.test.util.ReflectionTestUtils;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

import static com.engati.data.analytics.sdk.enums.DataSourceMetaInfo.PRODUCT;

@RunWith(MockitoJUnitRunner.class)
public class IngestionHandlerServiceImplTest {

  @InjectMocks
  private IngestionHandlerServiceImpl ingestionHandlerService;

  private JsonObject jsonObject;

  @Mock
  private DruidIngestionServiceRetrofit druidIngestionServiceRetrofit;

  @Captor
  private ArgumentCaptor<RequestBody> requestBodyCaptor;

  @Before
  public void init() throws IllegalAccessException {
    MockitoAnnotations.initMocks(this);
    jsonObject = new JsonObject();
    jsonObject.addProperty("task", "1234342");
  }

  @Test
  public void test_ingestToDruidSuccess() throws IOException {
    ReflectionTestUtils.setField(ingestionHandlerService, "netChangePath", "net_change/%s");
    ReflectionTestUtils.setField(ingestionHandlerService, "initialLoadPath", "initial_load");
    ReflectionTestUtils
        .setField(ingestionHandlerService, "ingestionSpecsPath", "ingestionSpecs/%s");
    Call druidMockedCall = Mockito.mock(Call.class);
    Mockito.when(druidIngestionServiceRetrofit.ingestDataToDruid(Mockito.any()))
        .thenReturn(druidMockedCall);
    Response<JsonObject> druidResponse = Response.success(jsonObject);
    Mockito.when(druidMockedCall.execute()).thenReturn(druidResponse);
    DataAnalyticsEngineResponse<DruidIngestionResponse> dataAnalyticsEngineResponse =
        ingestionHandlerService.ingestToDruid(1234L, 5678L, null, PRODUCT.name(), Boolean.FALSE);
    Assert.assertEquals(dataAnalyticsEngineResponse.getStatus(),
        DataAnalyticsEngineStatusCode.INGESTION_SUCCESS);
    Mockito.verify(druidIngestionServiceRetrofit).ingestDataToDruid(requestBodyCaptor.capture());
  }

  @Test
  public void test_ingestToDruidFailure() throws IOException {
    ReflectionTestUtils.setField(ingestionHandlerService, "netChangePath", "net_change/%s");
    ReflectionTestUtils.setField(ingestionHandlerService, "initialLoadPath", "initial_load");
    ReflectionTestUtils
        .setField(ingestionHandlerService, "ingestionSpecsPath", "ingestionSpecs/%s");
    Call druidMockedCall = Mockito.mock(Call.class);
    Mockito.when(druidIngestionServiceRetrofit.ingestDataToDruid(Mockito.any()))
        .thenReturn(druidMockedCall);
    Response<JsonObject> druidResponse = Response.success(null);
    Mockito.when(druidMockedCall.execute()).thenReturn(druidResponse);
    DataAnalyticsEngineResponse<DruidIngestionResponse> dataAnalyticsEngineResponse =
        ingestionHandlerService.ingestToDruid(1234L, 5678L, null, PRODUCT.name(), Boolean.FALSE);
    Assert.assertEquals(dataAnalyticsEngineResponse.getStatus(),
        DataAnalyticsEngineStatusCode.INGESTION_FAILURE);
    Mockito.verify(druidIngestionServiceRetrofit).ingestDataToDruid(requestBodyCaptor.capture());
  }

  @After
  public void tearDown() {
    Mockito.verifyNoMoreInteractions(druidIngestionServiceRetrofit);
  }
}
