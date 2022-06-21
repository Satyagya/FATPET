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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.engati.data.analytics.engine.Utils.CommonUtils.MAPPER;
import static java.lang.Double.parseDouble;


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
    long diffInMillies = Math.abs(
        dashboardRequest.getEndTime().getTime() - dashboardRequest.getStartTime().getTime());
    long gap = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    double presentValue = 0.0;
    double percentageChange = 0.0;
    double pastValue = 0.0;

    Map<String, String> query_params = new HashMap<>();
    query_params.put(QueryConstants.GAP, String.valueOf(gap));
    query_params.put(QueryConstants.DATE, endDate);
    query_params.put(Constants.BOT_REF, botRef.toString());
    presentValue = executeQueryForDashboardFlier(NativeQueries.GET_ENGAGED_USERS, query_params,
        QueryConstants.USERS, botRef);

    query_params.put(QueryConstants.DATE, startDate);
    pastValue = executeQueryForDashboardFlier(NativeQueries.GET_ENGAGED_USERS, query_params,
        QueryConstants.USERS, botRef);

    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).build());
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    return response;
  }

  public double percentageChange(double final_value, double initial_value) {
    if (initial_value == (double) 0) {
      return 100 * final_value;
    } else
      return 100 * ((final_value - initial_value) / (double) initial_value);
  }

  private double executeQueryForDashboardFlier(String query, Map<String, String> query_params,
      String metric_name, Long botRef) {
    for (Map.Entry<String, String> query_param : query_params.entrySet()) {
      query = query.replace(query_param.getKey(), query_param.getValue());
    }
    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, query);
    log.debug("Request body for query to duckDB: {}", requestBody);
    double value = 0;
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        String responseString =
            MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class)
                .get(Constants.RESPONSE_OBJECT).get(metric_name).toString();
        value = parseDouble(String.valueOf(
            MAPPER.readValue(responseString, ArrayList.class).stream().findFirst().get()));
      }
    } catch (Exception e) {
      log.error("Error executing query :{} with botRef: {}", query, botRef, e);
    }
    return value;
  }


}

