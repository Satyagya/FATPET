package com.engati.data.analytics.engine.ingestionHandler.impl;

import com.engati.data.analytics.engine.ingestionHandler.IngestionHandlerService;
import com.engati.data.analytics.engine.response.ingestion.IngestionResponse;
import com.engati.data.analytics.engine.response.ingestion.IngestionStatusCode;
import com.engati.data.analytics.engine.response.ingestion.UserIngestionProcess;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import okhttp3.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.io.FileReader;
import java.io.IOException;
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
  private DruidServiceRetrofit druidServiceRetrofit;


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
  public IngestionResponse<UserIngestionProcess> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad) {
    UserIngestionProcess userIngestionProcess =
        new UserIngestionProcess().builder().botRef(botRef).customerId(customerId).taskId(null)
            .build();
    IngestionResponse<UserIngestionProcess> ingestionResponse =
        new IngestionResponse<>(IngestionStatusCode.FAIL);
    ingestionResponse.setResponseObject(userIngestionProcess);
    Map<String, String> replaceMap = new HashMap<>();
    replaceMap.put(CUSTOMER_ID_PLACE_HOLDER, customerId.toString());
    replaceMap.put(BOTREF_PLACE_HOLDER, botRef.toString());
    replaceMap.put(DIR_PATH_PLACE_HOLDER, "dir_path");
    String requestBody = replacePlaceHolders("ingestion_spec_path", replaceMap);
    JsonObject druidResponse = ingestionRequestToDruid(requestBody);
    if (Objects.nonNull(druidResponse)) {
      userIngestionProcess.setTaskId(druidResponse.get(TASK_ID).getAsString());
      ingestionResponse.setStatus(IngestionStatusCode.SUCCESS);
      log.info("Ingestion to druid successful for customerId:{}, botRef:{}, timestamp:{} and "
          + "dataSourceName:{}", customerId, botRef, timestamp, dataSourceName);
    } else {
      log.info("Ingestion to druid failed for customerId:{}, botRef:{}, timestamp:{} and "
          + "dataSourceName:{}", customerId, botRef, timestamp, dataSourceName);
    }
    return ingestionResponse;
  }


  /**
   * Returns json response from druid for the request
   *
   * @param requestBody
   *
   * @return
   */
  private JsonObject ingestionRequestToDruid(String requestBody) {
    JsonObject output = null;
    try {
      RequestBody body = RequestBody
          .create(okhttp3.MediaType.parse("application/json; charset=utf-8"), requestBody);
      Response<JsonObject> response;
      response = druidServiceRetrofit.ingestDataToDruid(body).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        log.info("response: {}", response);
        output = response.body();
      } else {
        log.error("Failed to get response errorBody:{}", response.errorBody().string());
      }
    } catch (IOException e) {
      log.error("Failed to get response", e);
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
  private String replacePlaceHolders(String ingestionFileName, Map<String, String> replaceMap) {
    String requestBody = null;
    try {
      requestBody =
          new JsonParser().parse(new FileReader(ingestionFileName)).getAsJsonObject().toString();
      for (Map.Entry<String, String> field : replaceMap.entrySet()) {
        requestBody = requestBody.replace(field.getKey(), field.getValue());
      }
    } catch (Exception e) {
      log.error("Error while replacing place holder from the filename:{} with replaceMap:{}",
          ingestionFileName, replaceMap.toString(), e);
    }
    return requestBody;
  }
}
