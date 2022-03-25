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
      query = query.replace("number_of_results", productDiscoveryRequest.getNumberOfVariantsInResponse().toString());
      if(productDiscoveryRequest.getCollection() != null){
        query = query.replace("--add collection--", "and collection = '"+productDiscoveryRequest.getCollection()+"'");
        query = query.replace("--add product_type--", "");
        query = query.replace("--add product_tags--", "");
        query = query.replace("--add productIds--", "");
      }
      else if (productDiscoveryRequest.getProductType() != null){
        query = query.replace("--add product_type--", "and product_type = '"+productDiscoveryRequest.getProductType()+"'");
        query = query.replace("--add collection--", "");
        query = query.replace("--add product_tags--", "");
        query = query.replace("--add productIds--", "");

      }
      else if (productDiscoveryRequest.getProductTag() != null){
        query = query.replace("--add product_tags--", "and product_tags in '%"+productDiscoveryRequest.getProductTag()+"%'");
        query = query.replace("--add collection--", "");
        query = query.replace("--add product_type--", "");
        query = query.replace("--add productIds--", "");
      }
      else if (!CollectionUtils.isEmpty(productDiscoveryRequest.getProductIds())){
        query = query.replace("--add product_tags--", "");
        query = query.replace("--add collection--", "");
        query = query.replace("--add product_type--", "");
        query = query.replace("--add productIds--", " and product_id in "+productDiscoveryRequest.getProductIds());
        query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
        query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      }
      else{
        query = query.replace("--add collection--", "");
        query = query.replace("--add product_tags--", "");
        query = query.replace("--add product_type--", "");
        query = query.replace("--add productIds--", "");

      }
    Map<Long, Map<String, Object>> productVariants = CommonUtils.executeQueryForDetails(query, "variant_id");
      for ( Map.Entry<Long, Map<String, Object>> productVariant : productVariants.entrySet()){
        Map<String, Object> variantMap = productVariant.getValue();
        ProductVariantResponse productVariantResponse = new ProductVariantResponse();
        productVariantResponse.setVariantId((Long) variantMap.get("variant_id"));
        productVariantResponse.setProductId((Long) variantMap.get("product_id"));
        responseList.add(productVariantResponse);
      }
      response.setResponseObject(responseList);
      response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
      return response;
  }
}

