package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TimeSeriesQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.join.JoinTimeSeriesMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CartAbandonmentRateMetric extends MetricHandler {

  private static final String METRIC_HANDLER_NAME = "cart_abandonment";

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Override
  public String getMetricName() {
    return METRIC_HANDLER_NAME;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    List<QueryResponse> responses = new ArrayList<>();
    MultiQueryMetaInfo multiQueryMetaInfo = ((MultiQueryMetaInfo) druidQueryMetaInfo);
    QueryResponse response = new QueryResponse();
    for (DruidQueryMetaInfo druidQuery: multiQueryMetaInfo.getMultiMetricQuery()) {
      response = queryHandlerFactory.getQueryHandler(druidQuery.getType(),
          botRef, customerId).generateAndExecuteQuery(botRef, customerId,
          druidQuery, response);

      if(druidQuery instanceof TimeSeriesQueryMetaInfo || druidQuery instanceof JoinTimeSeriesMetaInfo){
        response = postprocess(response);
      }

    }
    return response;
  }

  private QueryResponse postprocess(QueryResponse response) {
    SimpleResponse simpleResponse = (SimpleResponse) response;
    simpleResponse.getQueryResponse();

    for(String timeKey :simpleResponse.getQueryResponse().keySet()){
      Map<String, Object> item = simpleResponse.getQueryResponse().get(timeKey).get(0);
      Integer initiated_sales = (Integer) item.get(Constants.INITIATED_SALES);
      if(initiated_sales == 0){
        item.put(Constants.CART_ABANDONMENT, "N/A");
      }
    }
    return response;
  }
}
