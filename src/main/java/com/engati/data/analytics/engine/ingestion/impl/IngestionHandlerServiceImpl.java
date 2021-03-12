package com.engati.data.analytics.engine.ingestion.impl;


import com.engati.data.analytics.engine.ingestion.IngestionHandlerService;
import com.engati.data.analytics.engine.retrofit.DruidIngestionServiceRetrofit;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.enums.DataSourceMetaInfo;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import okhttp3.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import retrofit2.Response;
import sun.misc.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.engati.data.analytics.engine.constants.DruidConstants.BOTREF_PLACE_HOLDER;
import static com.engati.data.analytics.engine.constants.DruidConstants.CUSTOMER_ID_PLACE_HOLDER;
import static com.engati.data.analytics.engine.constants.DruidConstants.DIR_PATH_PLACE_HOLDER;
import static com.engati.data.analytics.engine.constants.DruidConstants.TASK_ID;

@Slf4j
@Service("com.engati.data.analytics.engine.ingestionHandler.impl.IngestionHandlerImpl")
public class IngestionHandlerServiceImpl implements IngestionHandlerService {

  @Autowired
  private DruidIngestionServiceRetrofit druidIngestionServiceRetrofit;

  @Value("${s3.initial.load.bucket}")
  private String initialLoadPath;

  @Value("${s3.net.load.bucket}")
  private String netChangePath;

  @Value("${druid.ingestion.specs.path}")
  private String ingestionSpecsPath;


  /**
   * returns task id for the successful druid ingestion
   *
   * @param customerId     customer identifier
   * @param botRef         bot reference
   * @param timestamp      timestamp (from date)
   * @param dataSourceName name of the data source
   * @param isInitialLoad  whether it is initial load or not
   *
   * @return
   */
  @Override
  public DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId,
      Long botRef, String timestamp, String dataSourceName, Boolean isInitialLoad) {
    DruidIngestionResponse druidIngestionResponse =
        new DruidIngestionResponse().builder().botRef(botRef).customerId(customerId).build();
    DataAnalyticsEngineResponse<DruidIngestionResponse> dataAnalyticsEngineResponse =
        new DataAnalyticsEngineResponse<>(DataAnalyticsEngineStatusCode.INGESTION_FAILURE);
    dataAnalyticsEngineResponse.setResponseObject(druidIngestionResponse);
    Map<String, String> replaceMap = new HashMap<>();
    replaceMap.put(CUSTOMER_ID_PLACE_HOLDER, customerId.toString());
    replaceMap.put(BOTREF_PLACE_HOLDER, botRef.toString());
    if (isInitialLoad) {
      replaceMap.put(DIR_PATH_PLACE_HOLDER, initialLoadPath);
    } else {
      replaceMap.put(DIR_PATH_PLACE_HOLDER, String.format(netChangePath, timestamp));
    }
    String requestBody = replacePlaceHolders(customerId, botRef, String.format(ingestionSpecsPath,
        DataSourceMetaInfo.getIngestionPathForDataSource(dataSourceName)), replaceMap);
    JsonObject druidResponse = ingestionRequestToDruid(requestBody, customerId, botRef);
    if (Objects.nonNull(druidResponse)) {
      druidIngestionResponse.setTaskId(druidResponse.get(TASK_ID).getAsString());
      dataAnalyticsEngineResponse.setStatus(DataAnalyticsEngineStatusCode.INGESTION_SUCCESS);
      log.info("Ingestion to druid successful for customerId:{}, botRef:{}, timestamp:{} and "
          + "dataSourceName:{}", customerId, botRef, timestamp, dataSourceName);
    } else {
      log.error("Ingestion to druid failed for customerId:{}, botRef:{}, timestamp:{} and "
          + "dataSourceName:{}", customerId, botRef, timestamp, dataSourceName);
    }
    return dataAnalyticsEngineResponse;
  }


  /**
   * Returns json response from druid for the request
   *
   * @param requestBody
   *
   * @return
   */
  private JsonObject ingestionRequestToDruid(String requestBody, Long customerId, Long botRef) {
    JsonObject output = null;
    try {
      RequestBody body = RequestBody
          .create(okhttp3.MediaType.parse("application/json; charset=utf-8"), requestBody);
      Response<JsonObject> response;
      response = druidIngestionServiceRetrofit.ingestDataToDruid(body).execute();
      log.info(
          "Request body :{}, response body:{}, response code :{} for customerId:{} and botRef:{}",
          requestBody, response.toString(), response.code(), customerId, botRef);
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        log.info("response for customerId:{}, botRef:{} is {}", customerId, botRef, response);
        output = response.body();
      } else {
        log.error("Failed to get response for customerId:{}, botRef:{}, errorBody:{}", customerId,
            botRef, response.errorBody().string());
      }
    } catch (IOException e) {
      log.error("Failed to get response for customerId:{}, botRef:{}", customerId, botRef, e);
    }
    return output;
  }

  /**
   * Responsible for replacing all the place holders with provided configs in the ingestion spec
   * json
   *
   * @param ingestionFileName
   * @param replaceMap
   *
   * @return
   */
  private String replacePlaceHolders(Long customerId, Long botRef, String ingestionFileName,
      Map<String, String> replaceMap) {
    String requestBody = null;
    try {
      InputStream inputStream = getClass().getClassLoader().getResourceAsStream(ingestionFileName);
      if (Objects.nonNull(inputStream)) {
        byte[] bytes = IOUtils.readFully(inputStream, Integer.MAX_VALUE, true);
        requestBody = new String(bytes);
        for (Map.Entry<String, String> field : replaceMap.entrySet()) {
          requestBody = requestBody.replace(field.getKey(), field.getValue());
        }
      } else {
        log.error("Ingestion file: {} not found for cid: {} botRef: {}", ingestionFileName,
            customerId, botRef);
      }
    } catch (IOException e) {
      log.error("Exception while replacing place holder from the filename:{} with replaceMap:{} for "
              + "customerId:{} and botRef:{}", ingestionFileName, replaceMap.toString(), customerId,
          botRef, e);
    }
    return requestBody;
  }
}
