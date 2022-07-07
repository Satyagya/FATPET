package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.EtlEngineRestUtility;
import com.engati.data.analytics.engine.Utils.PdeRestUtility;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardChartResponse;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;
import com.engati.data.analytics.engine.model.response.DashboardGraphResponse;
import com.engati.data.analytics.engine.model.response.DashboardProductResponse;
import com.engati.data.analytics.engine.model.response.PdeProductResponse;
import com.engati.data.analytics.engine.repository.DashboardRepository;
import com.engati.data.analytics.engine.service.DashboardService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.engati.data.analytics.engine.Utils.CommonUtils.MAPPER;
import static java.lang.Double.parseDouble;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;


@Slf4j
@Service("com.engati.data.analytics.engine.service.DashboardService")
public class DashboardServiceImpl implements DashboardService {

  @Autowired
  private EtlEngineRestUtility etlEngineRestUtility;

  @Autowired
  private DashboardRepository dashboardRepository;

  @Autowired
  private PdeRestUtility pdeRestUtility;

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
    response.setStatus(ResponseStatusCode.SUCCESS);
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
    response.setStatus(ResponseStatusCode.SUCCESS);
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
    presentValue =
        executeQueryForDashboardFlier(NativeQueries.GET_AOV, query_params, QueryConstants.AOV,
            botRef);

    query_params.put(QueryConstants.DATE, startDate);
    pastValue =
        executeQueryForDashboardFlier(NativeQueries.GET_AOV, query_params, QueryConstants.AOV,
            botRef);

    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getAbandonedCheckouts(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting Abandoned Checkouts for botRef: {} for timeRanges between "
            + "{} and " + "{}", botRef, dashboardRequest.getStartTime(),
        dashboardRequest.getEndTime());
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
      presentValue =
          dashboardRepository.getAbandonedCheckoutsbyBotRefbyTimeRange(botRef, endDate, gap);
    } catch (Exception e) {
      log.error("Error getting abandonedCheckout for botRef: {} for date: {}", botRef, endDate, e);
    }
    try {
      pastValue =
          dashboardRepository.getAbandonedCheckoutsbyBotRefbyTimeRange(botRef, startDate, gap);
    } catch (Exception e) {
      log.error("Error getting abandonedCheckout for botRef: {} for date: {}", botRef, startDate,
          e);
    }
    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getTransactionsFromEngati(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting Transactions from Engati for botRef: {} for timeRanges between "
            + "{} and " + "{}", botRef, dashboardRequest.getStartTime(),
        dashboardRequest.getEndTime());
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
    presentValue = executeQueryForDashboardFlier(NativeQueries.GET_TRANSACTIONS_FROM_ENGATI, query_params,
        QueryConstants.TRANSACTIONS, botRef);

    query_params.put(QueryConstants.DATE, startDate);
    pastValue = executeQueryForDashboardFlier(NativeQueries.GET_TRANSACTIONS_FROM_ENGATI, query_params,
        QueryConstants.TRANSACTIONS, botRef);

    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardFlierResponse> getTransactionRevenueFromEngati(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting Transaction Revenue from Engati for botRef: {} for timeRanges between "
            + "{} and " + "{}", botRef, dashboardRequest.getStartTime(),
        dashboardRequest.getEndTime());
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
    presentValue = executeQueryForDashboardFlier(NativeQueries.GET_TRANSACTION_REVENUE_FROM_ENGATI, query_params,
        QueryConstants.TRANSACTION_REVENUE, botRef);

    query_params.put(QueryConstants.DATE, startDate);
    pastValue = executeQueryForDashboardFlier(NativeQueries.GET_TRANSACTION_REVENUE_FROM_ENGATI, query_params,
        QueryConstants.TRANSACTION_REVENUE, botRef);

    percentageChange = percentageChange(presentValue, pastValue);
    response.setResponseObject(DashboardFlierResponse.builder().presentValue(presentValue)
        .percentageChange(percentageChange).currency(currency).build());
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<DashboardProductResponse>> getMostPurchasedProducts(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info("Request received for getting most purchased Products for botRef: {} for timeRanges "
            + "between {} and " + "{}", botRef, dashboardRequest.getStartTime(),
        dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<DashboardProductResponse>> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    String query = NativeQueries.MOST_PURCHASED_PRODUCTS;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(QueryConstants.START_DATE, startDate);
    query = query.replace(QueryConstants.END_DATE, endDate);

    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, query);
    List<DashboardProductResponse> dashboardProductResponseList;
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        List<Long> productList = MAPPER.readValue(
            etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.PRODUCT_ID).toString(),
            List.class);

        dashboardProductResponseList = getProductDetails(productList, botRef);

        if (Objects.isNull(dashboardProductResponseList)) {
          response.setStatus(ResponseStatusCode.NO_PRODUCTS_FOUND);
        } else {
          response.setStatus(ResponseStatusCode.SUCCESS);
          response.setResponseObject(dashboardProductResponseList);
        }
      }
    } catch (Exception e) {
      log.error("Error executing query :{} with botRef: {}", query, botRef, e);
    }
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<DashboardProductResponse>> getMostAbandonedProducts(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info("Request received for getting most purchased Products for botRef: {} for timeRanges "
            + "between {} and " + "{}", botRef, dashboardRequest.getStartTime(),
        dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<DashboardProductResponse>> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    List<Long> productList = null;
    List<DashboardProductResponse> dashboardProductResponseList;
    try {
      productList =
          dashboardRepository.getMostAbandonedProductsByBotRef(botRef, startDate, endDate);

      dashboardProductResponseList = getProductDetails(productList, botRef);

      if (Objects.isNull(dashboardProductResponseList)) {
        response.setStatus(ResponseStatusCode.NO_PRODUCTS_FOUND);
      } else {
        response.setStatus(ResponseStatusCode.SUCCESS);
        response.setResponseObject(dashboardProductResponseList);
      }

    } catch (Exception e) {
      log.error("Error getting most abandonedProducts for botRef: {}", botRef, e);
    }

    return response;
  }

  @Override
  public DataAnalyticsResponse<List<DashboardGraphResponse>> getBotQueriesChart(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting botQueries Chart for botRef: {} for timeRanges between {} "
            + "and " + "{}", botRef, dashboardRequest.getStartTime(),
        dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<DashboardGraphResponse>> response = new DataAnalyticsResponse<>();
    List<DashboardGraphResponse> dashboardGraphResponseList = new ArrayList<>();
    long diffInMillies = Math.abs(
        dashboardRequest.getEndTime().getTime() - dashboardRequest.getStartTime().getTime());
    long gap = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());

    if (gap <= 20) {
      String query = NativeQueries.BOT_QUERIES_COUNTS;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.START_DATE, startDate);
      query = query.replace(QueryConstants.END_DATE, endDate);
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      try {
        Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
        if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
            etlResponse.body())) {
          JSONObject queryResponse = MAPPER.readValue(MAPPER.writeValueAsString(
              MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class)
                  .get(Constants.RESPONSE_OBJECT)), JSONObject.class);
          ArrayList<String> queryRequestDate =
              (ArrayList<String>) getDatesBetweenRange(startDate, endDate);
          ArrayList queryResponseDate = (ArrayList) queryResponse.get(Constants.CREATED_DATE);
          ArrayList queryResponseQueriesAsked =
              (ArrayList) queryResponse.get(Constants.QUERIES_ASKED);
          ArrayList queryResponseQueriesUnanswered =
              (ArrayList) queryResponse.get(Constants.QUERIES_UNANSWERED);
          int dataPoints = queryRequestDate.size();
          for (int i = 0; i < dataPoints; i++) {
            int responseIndex = queryResponseDate.indexOf(queryRequestDate.get(i));
            if (responseIndex == -1) {
              DashboardGraphResponse dashboardGraphResponse =
                  DashboardGraphResponse.builder().date(queryRequestDate.get(i))
                      .queriesAsked((double) 0).queriesUnanswered((double) 0).build();
              dashboardGraphResponseList.add(dashboardGraphResponse);
            } else {
              DashboardGraphResponse dashboardGraphResponse = DashboardGraphResponse.builder()
                  .date(queryResponseDate.get(responseIndex).toString()).queriesAsked(
                      Double.valueOf(queryResponseQueriesAsked.get(responseIndex).toString()))
                  .queriesUnanswered(
                      Double.valueOf(queryResponseQueriesUnanswered.get(responseIndex).toString()))
                  .build();
              dashboardGraphResponseList.add(dashboardGraphResponse);
            }

          }
        } else {
          dashboardGraphResponseList = null;
          response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
          log.error("Error executing query :{} with botRef: {}", query, botRef);
        }
        response.setResponseObject(dashboardGraphResponseList);
        response.setStatus(ResponseStatusCode.SUCCESS);
      } catch (Exception e) {
        dashboardGraphResponseList = null;
        response.setResponseObject(dashboardGraphResponseList);
        response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
        log.error("Error executing query :{} with botRef: {}", query, botRef, e);
      }
    } else {
      String query = NativeQueries.BOT_QUERIES_COUNTS_AGGREGATED;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.START_DATE, startDate);
      query = query.replace(QueryConstants.END_DATE, endDate);
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      try {
        Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
        if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
            etlResponse.body())) {
          JSONObject queryResponse = MAPPER.readValue(MAPPER.writeValueAsString(
              MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class)
                  .get(Constants.RESPONSE_OBJECT)), JSONObject.class);
          ArrayList queryResponseDate = (ArrayList) queryResponse.get(Constants.CREATED_DATE);
          ArrayList queryResponseQueriesAsked =
              (ArrayList) queryResponse.get(Constants.QUERIES_ASKED);
          ArrayList queryResponseQueriesUnanswered =
              (ArrayList) queryResponse.get(Constants.QUERIES_UNANSWERED);
          int dataPoints = queryResponseDate.size();
          for (int i = 0; i < dataPoints; i++) {
            DashboardGraphResponse dashboardGraphResponse =
                DashboardGraphResponse.builder().date(queryResponseDate.get(i).toString())
                    .queriesAsked(Double.valueOf(queryResponseQueriesAsked.get(i).toString()))
                    .queriesUnanswered(
                        Double.valueOf(queryResponseQueriesUnanswered.get(i).toString())).build();
            dashboardGraphResponseList.add(dashboardGraphResponse);
          }
        } else {
          dashboardGraphResponseList = null;
          response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
          log.error("Error executing query :{} with botRef: {}", query, botRef);
        }
        response.setResponseObject(dashboardGraphResponseList);
        response.setStatus(ResponseStatusCode.SUCCESS);
      } catch (Exception e) {
        dashboardGraphResponseList = null;
        response.setResponseObject(dashboardGraphResponseList);
        response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
        log.error("Error executing query :{} with botRef: {}", query, botRef, e);
      }
    }
    return response;
  }

  public double percentageChange(double final_value, double initial_value) {
    double value = 0.0;
    if (initial_value == (double) 0) {
      value = 100 * final_value;
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

  private List<DashboardProductResponse> getProductDetails(List<Long> productList, Long botRef) {
    List<DashboardProductResponse> dashboardProductResponseList = new ArrayList<>();
    try {
      JSONObject pdeRequestBody =
          MAPPER.readValue(String.format(Constants.productDetailsRequest, productList.toString()),
              JSONObject.class);
      Response<JsonNode> productDetailsResponse =
          pdeRestUtility.getProductDetails(botRef, "daeDomain", pdeRequestBody).execute();

      if (Objects.nonNull(productDetailsResponse) && productDetailsResponse.isSuccessful()
          && Objects.nonNull(productDetailsResponse.body())) {

        JSONArray productDetailsArray = MAPPER.readValue(
            MAPPER.readValue(MAPPER.writeValueAsString(productDetailsResponse.body()),
                JsonNode.class).get(Constants.RESPONSE).toString(), JSONArray.class);

        for (Object product : productDetailsArray) {
          PdeProductResponse pdeProductResponse =
              MAPPER.readValue(MAPPER.writeValueAsString(product), PdeProductResponse.class);
          DashboardProductResponse dashboardProductResponse = new DashboardProductResponse();
          BeanUtils.copyProperties(pdeProductResponse, dashboardProductResponse);
          dashboardProductResponseList.add(dashboardProductResponse);
        }
      } else {
        return null;
      }
    } catch (Exception e) {
      log.error("Error while getting Product Details from PDE for BotRef: {}", botRef);
      return null;
    }
    return dashboardProductResponseList.stream().collect(collectingAndThen(toCollection(
            () -> new TreeSet<>(comparingLong(DashboardProductResponse::getPRODUCT_productId))),
        ArrayList::new));
  }

  @Override
  public DataAnalyticsResponse<DashboardChartResponse> getEngagedUsersPerPlatform(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting Engaged Users for platforms for botRef: {} for timeRanges between {} and "
            + "{}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardChartResponse> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    Map<String, String> query_params = new HashMap<>();
    query_params.put(QueryConstants.START_DATE, startDate);
    query_params.put(QueryConstants.END_DATE, endDate);
    query_params.put(Constants.BOT_REF, botRef.toString());
    DashboardChartResponse dashboardChartResponse = executeQueryForDashboardChart(NativeQueries.GET_ENGAGED_USERS_BY_PLATFORM,
        query_params,
        QueryConstants.USERS, QueryConstants.PLATFORM, botRef);
    response.setResponseObject(dashboardChartResponse);
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardChartResponse> getConversationIntentBreakdown(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting conversations intents breakdown for botRef: {} for timeRanges between {} and "
            + "{}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardChartResponse> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    Map<String, String> query_params = new HashMap<>();
    query_params.put(QueryConstants.START_DATE, startDate);
    query_params.put(QueryConstants.END_DATE, endDate);
    query_params.put(Constants.BOT_REF, botRef.toString());
    DashboardChartResponse dashboardChartResponse = executeQueryForDashboardChart(NativeQueries.GET_CONVERSATION_INTENT,
        query_params,
        QueryConstants.INTENT_COUNT_SUM, QueryConstants.INTENT_LABEL, botRef);
    response.setResponseObject(dashboardChartResponse);
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<DashboardChartResponse> getConversationSentimentBreakdown(Long botRef,
      DashboardRequest dashboardRequest) {
    log.info(
        "Request received for getting conversations sentiment breakdown for botRef: {} for timeRanges between {} and "
            + "{}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardChartResponse> response = new DataAnalyticsResponse<>();
    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);
    String startDate = formatter.format(dashboardRequest.getStartTime());
    String endDate = formatter.format(dashboardRequest.getEndTime());
    Map<String, String> query_params = new HashMap<>();
    query_params.put(QueryConstants.START_DATE, startDate);
    query_params.put(QueryConstants.END_DATE, endDate);
    query_params.put(Constants.BOT_REF, botRef.toString());
    DashboardChartResponse dashboardChartResponse = executeQueryForDashboardChart(NativeQueries.GET_CONVERSATION_SENTIMENT,
        query_params, QueryConstants.SENTIMENT_COUNT_SUM, QueryConstants.SENTIMENT_LABEL, botRef);
    response.setResponseObject(dashboardChartResponse);
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<String> getLastUpdatedOn(Long botRef) {
    log.info("Request received for getting Last Updated On for botRef: {}", botRef);
    DataAnalyticsResponse<String> response = new DataAnalyticsResponse<>();
    String lastUpdatedOn = "";
    try {
      lastUpdatedOn = dashboardRepository.getLastUpdatedOn(botRef);
    } catch (Exception e) {
      log.error("Error getting LastUpdatedOn from the DB");
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
    }
    if (lastUpdatedOn == null){
      Timestamp timestamp = new Timestamp(System.currentTimeMillis());
      lastUpdatedOn = Constants.TIME_FORMAT.format(timestamp);
    }
    response.setResponseObject((lastUpdatedOn+"+0000").replace(" ","T"));
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  private DashboardChartResponse executeQueryForDashboardChart(String query, Map<String, String> query_params,
      String metric_name,String filterBy, Long botRef) {
    for (Map.Entry<String, String> query_param : query_params.entrySet()) {
      query = query.replace(query_param.getKey(), query_param.getValue());
    }
    DashboardChartResponse dashboardChartResponse = null;
    List<Integer> countPerMetric = new ArrayList<>();
    List<String> metrics = new ArrayList<>();
    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, query);
    log.debug("Request body for query to duckDB: {}", requestBody);
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        countPerMetric = MAPPER.readValue(MAPPER.readValue(etlResponse.body().toString(), JsonNode.class)
            .get(Constants.RESPONSE_OBJECT).get(metric_name)
            .toString(), ArrayList.class);
        metrics = MAPPER.readValue(MAPPER.readValue(etlResponse.body().toString(), JsonNode.class)
            .get(Constants.RESPONSE_OBJECT).get(filterBy)
            .toString(), ArrayList.class);
        dashboardChartResponse = getDashBoardChartResponse(countPerMetric, metrics);
      }
    } catch (Exception e) {
      log.error("Error executing query :{} with botRef: {}", query, botRef, e);
    }
    return dashboardChartResponse;
  }

  DashboardChartResponse getDashBoardChartResponse(List<Integer> countPerMetric, List<String> metricList){
    long totalCount = countPerMetric.stream()
        .mapToLong(Integer::longValue)
        .sum();
    Map<String,Long> metricPercentMap = new HashMap<>();
    for (int metricIndex = 0; metricIndex < countPerMetric.size(); metricIndex++) {
      long percent = countPerMetric.get(metricIndex)*100/totalCount ;
      metricPercentMap.put(metricList.get(metricIndex), percent);
    }
    return DashboardChartResponse.builder().metricPercentage(metricPercentMap).build();
  }

  public static List<String> getDatesBetweenRange(String startDate, String endDate) {

    LocalDate startLocalDate = LocalDate.parse(startDate);
    LocalDate endLocalDate = LocalDate.parse(endDate);
    long numOfDaysBetween = ChronoUnit.DAYS.between(startLocalDate, endLocalDate) + 1;
    return IntStream.iterate(0, i -> i + 1).limit(numOfDaysBetween)
        .mapToObj(i -> startLocalDate.plusDays(i).toString()).collect(Collectors.toList());

  }
}

