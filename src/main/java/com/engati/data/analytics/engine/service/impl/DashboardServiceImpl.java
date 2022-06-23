package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.EtlEngineRestUtility;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;
import com.engati.data.analytics.engine.model.response.DashboardProductResponse;
import com.engati.data.analytics.engine.repository.DashboardRepository;
import com.engati.data.analytics.engine.service.DashboardService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  @Autowired
  private DashboardRepository dashboardRepository;

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
    String currency = "";

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
        .percentageChange(percentageChange).currency(currency).build());
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getOrderCount(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting OrderCounts for botRef: {} for timeRanges between {} and "
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
    String currency = "";

    Map<String, String> query_params = new HashMap<>();
    query_params.put(QueryConstants.GAP, String.valueOf(gap));
    query_params.put(QueryConstants.DATE, endDate);
    query_params.put(Constants.BOT_REF, botRef.toString());
    presentValue = executeQueryForDashboardFlier(NativeQueries.GET_ORDER_COUNTS, query_params,
        QueryConstants.ORDERS, botRef);

    query_params.put(QueryConstants.DATE, startDate);
    pastValue = executeQueryForDashboardFlier(NativeQueries.GET_ORDER_COUNTS, query_params,
        QueryConstants.ORDERS, botRef);

    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getAOV(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting AOV for botRef: {} for timeRanges between {} and " + "{}",
        botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
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
    String currency;
    try {
      currency = dashboardRepository.findCurrencyByBotRef(botRef);
    } catch (Exception e) {
      log.error("Error getting currency for botRef: {}", botRef, e);
      currency = "";
    }
    Map<String, String> query_params = new HashMap<>();
    query_params.put(QueryConstants.GAP, String.valueOf(gap));
    query_params.put(QueryConstants.DATE, endDate);
    query_params.put(Constants.BOT_REF, botRef.toString());
    presentValue = executeQueryForDashboardFlier(NativeQueries.GET_AOV, query_params, QueryConstants.AOV,
        botRef);

    query_params.put(QueryConstants.DATE, startDate);
    pastValue = executeQueryForDashboardFlier(NativeQueries.GET_AOV, query_params, QueryConstants.AOV,
        botRef);

    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getAbandonedCheckouts(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting Abandoned Checkouts for botRef: {} for timeRanges between {} and " + "{}",
        botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
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
    String currency = "";

    try {
      presentValue = dashboardRepository.getAbandonedCheckoutsbyBotRefbyTimeRange(botRef, endDate, gap);
    } catch (Exception e) {
      log.error("Error getting abandonedCheckout for botRef: {} for date: {}", botRef, endDate, e);
    }
    try {
      pastValue = dashboardRepository.getAbandonedCheckoutsbyBotRefbyTimeRange(botRef, startDate, gap);
    } catch (Exception e) {
      log.error("Error getting abandonedCheckout for botRef: {} for date: {}", botRef, startDate, e);
    }
    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<Long>> getMostPurchasedProducts(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting most purchased Products for botRef: {} for timeRanges between {} and "
            + "{}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<Long>> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    String query = NativeQueries.MOST_PURCHASED_PRODUCTS;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(QueryConstants.START_DATE, startDate);
    query = query.replace(QueryConstants.END_DATE, endDate);

    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, query);
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        List<Long> productList = MAPPER.readValue(
            etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.PRODUCT_ID).toString(),
            List.class);
        response.setResponseObject(productList);
      }
    } catch (Exception e) {
      log.error("Error executing query :{} with botRef: {}", query, botRef, e);
    }
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<Long>> getMostAbandonedProducts(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting most purchased Products for botRef: {} for timeRanges between {} and "
            + "{}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<Long>> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    List<Long> productList = null;
    try {
      productList =
          dashboardRepository.getMostAbandonedProductsByBotRef(botRef, startDate, endDate);
    } catch (Exception e) {
      log.error("Error getting most abandonedProducts for botRef: {}", botRef, e);
    }
    response.setResponseObject(productList);
    return response;
  }

  public double percentageChange(double final_value, double initial_value) {
    double value = 0.0;
    if (initial_value == (double) 0) {
        value =  100 * final_value;
    } else {
      value = 100 * ((final_value - initial_value) / (double) initial_value);
    }
    return Math.round(value * 100.0) / 100.0;
  }

  private double executeQueryForDashboardFlier(String query, Map<String, String> query_params,
      String metric_name, Long botRef) {
    for (Map.Entry<String, String> query_param : query_params.entrySet()) {
      query = query.replace(query_param.getKey(), query_param.getValue());
    }
    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, query);
    log.info("Request body for query to duckDB: {}", requestBody);
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

