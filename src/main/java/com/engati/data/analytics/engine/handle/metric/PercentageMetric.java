package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TimeSeriesQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TopNQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.join.JoinTopNMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.engati.data.analytics.engine.constants.DruidConstants.PERCENTAGE_SUFFIX;

@Slf4j
@Service
public class PercentageMetric extends MetricHandler {

  private static final String METRIC_HANDLER_PERCENTAGE_CONTRIBUTION = "percentage_contribution";

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;


  @Override
  public String getMetricName() {
    return METRIC_HANDLER_PERCENTAGE_CONTRIBUTION;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    List<QueryResponse> responses = new ArrayList<>();
    MultiQueryMetaInfo multiQueryMetaInfo = ((MultiQueryMetaInfo) druidQueryMetaInfo);
    String grain = null;
    List<String> timeRange = Collections.emptyList();
    QueryResponse timeSeriesResponse = new QueryResponse();
    QueryResponse topNQueryResponse = new QueryResponse();
    QueryResponse joinTopNQueryResponse = new QueryResponse();
    for (DruidQueryMetaInfo druidQuery : multiQueryMetaInfo.getMultiMetricQuery()) {
      if (druidQuery instanceof TimeSeriesQueryMetaInfo) {
        timeSeriesResponse =
            queryHandlerFactory.getQueryHandler(druidQuery.getType(), botRef, customerId)
                .generateAndExecuteQuery(botRef, customerId, druidQuery, timeSeriesResponse);
      } else if (druidQuery instanceof TopNQueryMetaInfo) {
        topNQueryResponse =
            queryHandlerFactory.getQueryHandler(druidQuery.getType(), botRef, customerId)
                .generateAndExecuteQuery(botRef, customerId, druidQuery, topNQueryResponse);
      }
      else if (druidQuery instanceof JoinTopNMetaInfo) {
        joinTopNQueryResponse =
            queryHandlerFactory.getQueryHandler(druidQuery.getType(), botRef, customerId)
                .generateAndExecuteQuery(botRef, customerId, druidQuery, topNQueryResponse);
      }

    }
    return computePercentageContribution(timeSeriesResponse, topNQueryResponse, joinTopNQueryResponse,
        multiQueryMetaInfo.getMetricList().get(0));
  }

  private QueryResponse computePercentageContribution(QueryResponse timeSeriesResponse,
      QueryResponse topNQueryResponse, QueryResponse joinTopNQueryResponse, String metric) {
    SimpleResponse timeSeriesSimpleResponse = (SimpleResponse) timeSeriesResponse;
    SimpleResponse topNSimpleResponse =
        Objects.nonNull(topNQueryResponse.getType()) ? ((SimpleResponse) topNQueryResponse) : null;
    SimpleResponse joinTopNSimpleResponse = Objects.nonNull(joinTopNQueryResponse.getType()) ?
        ((SimpleResponse) joinTopNQueryResponse) :
        null;
    List<Map<String, Object>> simpleResponseList = new ArrayList<>();
    Double overallDoubleValue;
    SimpleResponse simpleResponse = new SimpleResponse();
    if (Objects.isNull(topNSimpleResponse) && Objects.isNull(joinTopNSimpleResponse)) {
      return topNSimpleResponse;
    }
    overallDoubleValue = Objects.nonNull(timeSeriesSimpleResponse.getQueryResponse())?
        ((Number) timeSeriesSimpleResponse.getQueryResponse().values().iterator().next().get(0)
            .get(metric)).doubleValue(): 0.0;
    if (overallDoubleValue == 0){
      log.error("overallMetricValue is zero");
      for (Map<String, Object> singleResponse : simpleResponseList) {
        String metricPercentage = metric.concat(PERCENTAGE_SUFFIX);
        singleResponse.put(metricPercentage, 0);
      }
    }
    else
    {
      simpleResponse =
          Objects.nonNull(topNSimpleResponse) ? topNSimpleResponse : joinTopNSimpleResponse;
      simpleResponseList = simpleResponse.getQueryResponse().values().iterator().next();
      for (Map<String, Object> singleResponse : simpleResponseList) {
        String metricPercentage = metric.concat(PERCENTAGE_SUFFIX);
        Double metricValue = ((Number) singleResponse.get(metric)).doubleValue();
        singleResponse.put(metricPercentage, (metricValue / overallDoubleValue) * 100);
      }
    }
    return simpleResponse;
  }

}