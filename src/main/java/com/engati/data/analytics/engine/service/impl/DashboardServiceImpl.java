package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.EtlEngineRestUtility;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;
import com.engati.data.analytics.engine.service.DashboardService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.engati.data.analytics.engine.Utils.CommonUtils.MAPPER;


@Slf4j
@Service("com.engati.data.analytics.engine.service.DashboardService")
public class DashboardServiceImpl implements DashboardService {

  @Autowired
  private EtlEngineRestUtility etlEngineRestUtility;

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getEngagedUsers(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting Engaged Users for botRef: {} for timeRanges between {} and "
            + "{}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardFlierResponse> response = new DataAnalyticsResponse<>();
    long diffInMillies = Math.abs(dashboardRequest.getEndTime().getTime()- dashboardRequest.getStartTime().getTime());
    Long gap = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    Double presentValue = Double.valueOf(0);
    Double percentageChange = Double.valueOf(0);
    Double pastValue = Double.valueOf(0);

    String query_current = NativeQueries.GET_ENGAGED_USERS;
    query_current = query_current.replace(QueryConstants.GAP, gap.toString());
    query_current = query_current.replace(QueryConstants.END_DATE, endDate);
    query_current = query_current.replace(Constants.BOT_REF, botRef.toString());
    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, query_current);
    log.debug("Request body for query to duckDB: {}", requestBody);
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        String responseString =
            MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class)
                .get(Constants.RESPONSE_OBJECT).get(QueryConstants.USERS).toString();
        presentValue =
            Double.valueOf(
                String.valueOf(MAPPER.readValue(responseString, ArrayList.class).stream().findFirst().get()));
      }
    } catch (Exception e) {
      log.error("Exception while getting Users for current period for botRef: {}", botRef, e);
    }

    String query_past = NativeQueries.GET_ENGAGED_USERS;
    query_past = query_past.replace(QueryConstants.GAP, gap.toString());
    query_past = query_past.replace(QueryConstants.END_DATE, startDate);
    query_past = query_past.replace(Constants.BOT_REF, botRef.toString());
    requestBody.put(Constants.QUERY, query_past);
    log.debug("Request body for query to duckDB: {}", requestBody);
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        String responseString =
            MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class)
                .get(Constants.RESPONSE_OBJECT).get(QueryConstants.USERS).toString();
        pastValue =
            Double.valueOf(
                String.valueOf(MAPPER.readValue(responseString, ArrayList.class).stream().findFirst().get()));
      }
    } catch (Exception e) {
      log.error("Exception while getting Users for current period for botRef: {}", botRef, e);
    }
    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue).percentageChange(percentageChange).build());
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    return response;
  }

  public double percentageChange(Double final_value, Double initial_value){
    if (initial_value.equals((double) 0)){
      return 100 * final_value;
    }else return 100 * ((final_value - initial_value)/ (double) initial_value);
  }




}

