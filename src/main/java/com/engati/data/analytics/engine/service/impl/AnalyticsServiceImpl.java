package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.request.PurchaseHistoryRequest;
import com.engati.data.analytics.engine.model.response.OrderDetailsResponse;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;
import com.engati.data.analytics.engine.service.AnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service("com.engati.data.analytics.engine.service.AnalyticsService")
public class AnalyticsServiceImpl implements AnalyticsService {


  @Override
  public DataAnalyticsResponse<List<ProductVariantResponse>> getVariantsByUnitSales(Long botRef, ProductDiscoveryRequest productDiscoveryRequest) {
    DataAnalyticsResponse<List<ProductVariantResponse>> response = new DataAnalyticsResponse<>();
    try {
      List<ProductVariantResponse> responseList = new ArrayList<>();
      String query = NativeQueries.PRODUCT_VARIANTS_BY_UNIT_SALES;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.NUMBER_OF_RESULTS, productDiscoveryRequest.getNumberOfVariantsInResponse().toString());
      if (productDiscoveryRequest.getCollection() != null) {
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, "and collection = '" + productDiscoveryRequest.getCollection() + "'");
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);
      } else if (productDiscoveryRequest.getProductType() != null) {
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, "and product_type = '" + productDiscoveryRequest.getProductType() + "'");
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);

      } else if (productDiscoveryRequest.getProductTag() != null) {
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, "and product_tags in '%" + productDiscoveryRequest.getProductTag() + "%'");
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);
      } else if (!CollectionUtils.isEmpty(productDiscoveryRequest.getProductIds())) {
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, " and product_id in " + productDiscoveryRequest.getProductIds());
        query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
        query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      } else {
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);
      }
      Map<Long, Map<String, Object>> productVariants = CommonUtils.executeQueryForDetails(query, Constants.VARIANT_ID);
      for (Map.Entry<Long, Map<String, Object>> productVariant : productVariants.entrySet()) {
        Map<String, Object> variantMap = productVariant.getValue();
        ProductVariantResponse productVariantResponse = new ProductVariantResponse();
        productVariantResponse.setVariantId((Long) variantMap.get(Constants.VARIANT_ID));
        productVariantResponse.setProductId((Long) variantMap.get(Constants.PRODUCT_ID));
        responseList.add(productVariantResponse);
      }
      response.setResponseObject(responseList);
      response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    }catch (Exception e) {
      log.error("Exception encountered while fetching variants by Unit Sales for botRef: {}",botRef, e);
      response.setResponseObject(null);
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
    }
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<OrderDetailsResponse>> getPurchaseHistory(PurchaseHistoryRequest purchaseHistoryRequest) {
    DataAnalyticsResponse<List<OrderDetailsResponse>> response = new DataAnalyticsResponse<>();
    try {
      List<OrderDetailsResponse> responseList = new ArrayList<>();
      String query = NativeQueries.PURCHASE_HISTORY;
      query = query.replace(Constants.BOT_REF, purchaseHistoryRequest.getBotRef().toString());

      if (purchaseHistoryRequest.getStartTime() == null && purchaseHistoryRequest.getEndTime() == null)
        query = query.replace(QueryConstants.CREATED_AT_WINDOW, QueryConstants.EMPTY_STRING);
      else if (purchaseHistoryRequest.getStartTime() == null && purchaseHistoryRequest.getEndTime() != null)
        query = query.replace(QueryConstants.CREATED_AT_WINDOW, QueryConstants.CREATED_AT_LT_BASE + purchaseHistoryRequest.getEndTime().toString() + QueryConstants.QUOTE_MARK);
      else if (purchaseHistoryRequest.getStartTime() != null && purchaseHistoryRequest.getEndTime() == null)
        query = query.replace(QueryConstants.CREATED_AT_WINDOW, QueryConstants.CREATED_AT_GT_BASE + purchaseHistoryRequest.getStartTime().toString() + QueryConstants.QUOTE_MARK);
      else {
        query = query.replace(QueryConstants.FROM_DATE, QueryConstants.QUOTE_MARK + purchaseHistoryRequest.getStartTime().toString() + QueryConstants.QUOTE_MARK);
        query = query.replace(QueryConstants.TO_DATE, QueryConstants.QUOTE_MARK + purchaseHistoryRequest.getEndTime().toString() + QueryConstants.QUOTE_MARK);
      }

      if (!CollectionUtils.isEmpty(purchaseHistoryRequest.getCollection()))
        query = query.replace(QueryConstants.COLLECTION_NAME, purchaseHistoryRequest.getCollection()
            .stream().collect(Collectors.joining("','", "'", "'")));
      else
        query = query.replace(QueryConstants.COLLECTION_BASE, QueryConstants.EMPTY_STRING);

      if (!CollectionUtils.isEmpty(purchaseHistoryRequest.getProductType()))
        query = query.replace(QueryConstants.PRODUCT_TYPE, purchaseHistoryRequest.getProductType()
            .stream().collect(Collectors.joining("','", "'", "'")));
      else
        query = query.replace(QueryConstants.PRODUCT_TYPE_BASE, QueryConstants.EMPTY_STRING);

      Map<Long, Map<String, Object>> purchaseHistoryDetails = CommonUtils.executeQueryForDetails(query, QueryConstants.LINE_ITEM_ID);
      for (Map.Entry<Long, Map<String, Object>> purchaseHistoryDetail : purchaseHistoryDetails.entrySet()) {
        Map<String, Object> purchaseHistoryEntry = purchaseHistoryDetail.getValue();
        OrderDetailsResponse orderDetailsResponse = new OrderDetailsResponse();
        orderDetailsResponse.setOrderId((Long) purchaseHistoryEntry.get(QueryConstants.ORDER_ID));
        orderDetailsResponse.setProductId((Long) purchaseHistoryEntry.get(QueryConstants.PRODUCT_ID));
        orderDetailsResponse.setVariantId((Long) purchaseHistoryEntry.get(QueryConstants.VARIANT_ID));
        orderDetailsResponse.setCustomerId((Long) purchaseHistoryEntry.get(QueryConstants.CUSTOMER_ID));
        orderDetailsResponse.setCollectionId((Long) purchaseHistoryEntry.get(QueryConstants.COLLECTION_ID));
        orderDetailsResponse.setBotRef((Long) purchaseHistoryEntry.get(QueryConstants.BOT_REF));
        orderDetailsResponse.setCreatedDate(String.valueOf(purchaseHistoryEntry.get(QueryConstants.CREATED_AT)));
        orderDetailsResponse.setPrice((Double) purchaseHistoryEntry.get(QueryConstants.LINE_ITEM_PRICE));
        responseList.add(orderDetailsResponse);
      }
      response.setResponseObject(responseList);
      response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    }catch (Exception e){
      log.error("Exception encountered while fetching purchase history for botRef: ",purchaseHistoryRequest.getBotRef(), e);
      response.setResponseObject(null);
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
    }
    return response;
  }
}

