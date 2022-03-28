package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;
import com.engati.data.analytics.engine.service.AnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("com.engati.data.analytics.engine.service.AnalyticsService")
public class AnalyticsServiceImpl implements AnalyticsService {


  @Override
  public DataAnalyticsResponse<List<ProductVariantResponse>> getVariantsByUnitSales(Long customerId, Long botRef, ProductDiscoveryRequest productDiscoveryRequest) {
    DataAnalyticsResponse<List<ProductVariantResponse>> response = new DataAnalyticsResponse<>();
    List<ProductVariantResponse> responseList = new ArrayList<>();
      String query = NativeQueries.PRODUCT_VARIANTS_BY_UNIT_SALES;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.NUMBER_OF_RESULTS, productDiscoveryRequest.getNumberOfVariantsInResponse().toString());
      if(productDiscoveryRequest.getCollection() != null){
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, "and collection = '"+productDiscoveryRequest.getCollection()+"'");
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);
      }
      else if (productDiscoveryRequest.getProductType() != null){
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, "and product_type = '"+productDiscoveryRequest.getProductType()+"'");
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);

      }
      else if (productDiscoveryRequest.getProductTag() != null){
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, "and product_tags in '%"+productDiscoveryRequest.getProductTag()+"%'");
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);
      }
      else if (!CollectionUtils.isEmpty(productDiscoveryRequest.getProductIds())){
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, " and product_id in "+productDiscoveryRequest.getProductIds());
        query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
        query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      }
      else{
        query = query.replace(QueryConstants.ADD_COLLECTION_DETAILS_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TYPE_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, QueryConstants.EMPTY_STRING);
        query = query.replace(QueryConstants.ADD_PRODUCT_IDS_TO_QUERY, QueryConstants.EMPTY_STRING);
      }
    Map<Long, Map<String, Object>> productVariants = CommonUtils.executeQueryForDetails(query, Constants.VARIANT_ID);
      for ( Map.Entry<Long, Map<String, Object>> productVariant : productVariants.entrySet()){
        Map<String, Object> variantMap = productVariant.getValue();
        ProductVariantResponse productVariantResponse = new ProductVariantResponse();
        productVariantResponse.setVariantId((Long) variantMap.get(Constants.VARIANT_ID));
        productVariantResponse.setProductId((Long) variantMap.get(Constants.PRODUCT_ID));
        responseList.add(productVariantResponse);
      }
      response.setResponseObject(responseList);
      response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
      return response;
  }
}

