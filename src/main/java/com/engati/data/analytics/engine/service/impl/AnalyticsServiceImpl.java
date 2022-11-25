package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.Utils.EtlEngineRestUtility;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.dto.CustomerSetResponseDTO;
import com.engati.data.analytics.engine.model.request.CustomerDetailsRequest;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.request.PurchaseHistoryRequest;
import com.engati.data.analytics.engine.model.response.CustomerDetailsResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.model.response.OrderDetailsResponse;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;
import com.engati.data.analytics.engine.service.AnalyticsService;
import com.engati.data.analytics.engine.service.PrometheusManagementService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import retrofit2.Response;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service("com.engati.data.analytics.engine.service.AnalyticsService")
public class AnalyticsServiceImpl implements AnalyticsService {

  @Autowired
  private CommonUtils commonUtils;

  @Autowired
  private EtlEngineRestUtility etlEngineRestUtility;

  public static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired
  private SegmentServiceImpl segmentService;

  @Autowired
  @Qualifier("com.engati.data.analytics.engine.service.impl.PrometheusManagementServiceImpl")
  private PrometheusManagementService prometheusManagementService;

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
        query = query.replace(QueryConstants.ADD_PRODUCT_TAG_TO_QUERY, "and product_tags like '%" + productDiscoveryRequest.getProductTag() + "%'");
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
      Map<Long, Map<String, Object>> productVariants;
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.VARIANT_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.
              executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects
              .nonNull(etlResponse.body())) {
        productVariants = MAPPER.readValue(
            MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Object>>>() {
            });
      } else {
        response.setResponseObject(null);
        response.setStatus(ResponseStatusCode.DUCK_DB_QUERY_FAILURE);
        return response;
      }

      for (Map.Entry<Long, Map<String, Object>> productVariant : productVariants.entrySet()) {
        Map<String, Object> variantMap = productVariant.getValue();
        ProductVariantResponse productVariantResponse = new ProductVariantResponse();
        productVariantResponse.setVariantId((Long) variantMap.get(Constants.VARIANT_ID));
        productVariantResponse.setProductId((Long) variantMap.get(Constants.PRODUCT_ID));
        responseList.add(productVariantResponse);
      }
      response.setResponseObject(responseList);
      response.setStatus(ResponseStatusCode.SUCCESS);
    }catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getVariantsByUnitSales", botRef,
          e.getMessage(), CommonUtils.getStringValueFromObject(productDiscoveryRequest));
      log.error("Exception encountered while fetching variants by Unit Sales for botRef: {}",botRef, e);
      response.setResponseObject(null);
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
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

      Map<Long, Map<String, Object>> purchaseHistoryDetails;
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, QueryConstants.LINE_ITEM_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.
              executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects
              .nonNull(etlResponse.body())) {
        purchaseHistoryDetails = MAPPER.readValue(
            MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Object>>>() {
            });
      } else {
        response.setResponseObject(null);
        response.setStatus(ResponseStatusCode.DUCK_DB_QUERY_FAILURE);
        return response;
      }

      for (Map.Entry<Long, Map<String, Object>> purchaseHistoryDetail : purchaseHistoryDetails.entrySet()) {
        Map<String, Object> purchaseHistoryEntry = purchaseHistoryDetail.getValue();
        OrderDetailsResponse orderDetailsResponse = new OrderDetailsResponse();
        orderDetailsResponse.setOrderId(Long.valueOf(purchaseHistoryEntry.get(QueryConstants.ORDER_ID).toString()));
        orderDetailsResponse.setProductId(Long.valueOf(purchaseHistoryEntry.get(QueryConstants.PRODUCT_ID).toString()));
        orderDetailsResponse.setVariantId(Long.valueOf(purchaseHistoryEntry.get(QueryConstants.VARIANT_ID).toString()));
        orderDetailsResponse.setCustomerId(Long.valueOf(purchaseHistoryEntry.get(QueryConstants.CUSTOMER_ID).toString()));
        orderDetailsResponse.setCollectionId( Objects.nonNull(purchaseHistoryEntry.get(QueryConstants.COLLECTION_ID)) ? Double.valueOf(purchaseHistoryEntry.get(QueryConstants.COLLECTION_ID).toString()).longValue(): null);
        orderDetailsResponse.setBotRef(Long.valueOf(purchaseHistoryEntry.get(QueryConstants.BOT_REF).toString()));
        orderDetailsResponse.setCreatedDate(String.valueOf(purchaseHistoryEntry.get(QueryConstants.CREATED_AT)));
        orderDetailsResponse.setPrice(Double.valueOf(purchaseHistoryEntry.get(QueryConstants.LINE_ITEM_PRICE).toString()));
        responseList.add(orderDetailsResponse);
      }
      response.setResponseObject(responseList);
      response.setStatus(ResponseStatusCode.SUCCESS);
    }catch (Exception e){
      prometheusManagementService.apiRequestFailureEvent("getPurchaseHistory",
          purchaseHistoryRequest.getBotRef(), e.getMessage(),
          CommonUtils.getStringValueFromObject(purchaseHistoryRequest));
      log.error("Exception encountered while fetching purchase history for botRef: {}",purchaseHistoryRequest.getBotRef(), e);
      response.setResponseObject(null);
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
    }
    return response;
  }

  @Override
  public DataAnalyticsResponse<CustomerDetailsResponse> getCustomerDetails(Long botRef,
      CustomerDetailsRequest customerDetailsRequest) {
    DataAnalyticsResponse<CustomerDetailsResponse> response = new DataAnalyticsResponse<>();
    try {
      CustomerDetailsResponse customerDetailsResponse = new CustomerDetailsResponse();
      log.info(" CustomerDetails Request {} ", customerDetailsRequest);
      if ((customerDetailsRequest.getCustomerEmail() == null || customerDetailsRequest.getCustomerEmail().equals("")) && (
          (customerDetailsRequest.getCustomerEmail() == null || customerDetailsRequest.getCustomerPhone().equals("")))) {
        response.setResponseObject(null);
        response.setStatus(ResponseStatusCode.INPUT_MISSING);
      } else {
        CustomerSetResponseDTO customerSetResponseDTO = getCustomerId(customerDetailsRequest, botRef);
        if (Objects.equals(customerSetResponseDTO.getStatus(), String.valueOf(ResponseStatusCode.PROCESSING_ERROR))) {
          response.setResponseObject(null);
          response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
        } else if (customerSetResponseDTO.getCustomerId() == 0L) {
          response.setResponseObject(null);
          response.setStatus(ResponseStatusCode.SUCCESS);
        } else {
          Set<Long> customerSet = new HashSet<>();
          customerSet.add(customerSetResponseDTO.getCustomerId());
          List<CustomerSegmentationResponse> customerSegmentationResponse =
              segmentService.getDetailsforCustomerSegments(customerSet, botRef);
          if (!customerSegmentationResponse.isEmpty()) {
            BeanUtils.copyProperties(customerDetailsResponse, customerSegmentationResponse.get(0));
            try {
              String query = NativeQueries.GET_LAST_ORDER_DATE_FOR_CUSTOMER;
              query = query.replace(Constants.CUSTOMER_PROVIDED,
                  customerSetResponseDTO.getCustomerId().toString());
              query = query.replace(Constants.BOTREF, botRef.toString());
              JSONObject requestBody = new JSONObject();
              requestBody.put(Constants.QUERY, query);
              Response<JsonNode> etlResponse =
                  etlEngineRestUtility.executeQuery(requestBody).execute();
              if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
                  etlResponse.body())) {
                String lastOrderDate =
                    (MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class)
                        .get(Constants.RESPONSE_OBJECT).get(Constants.LAST_ORDER_DATE).get(0).textValue());
                customerDetailsResponse.setLastOrderDate(lastOrderDate);
              } else {
                customerDetailsResponse.setLastOrderDate(String.valueOf(Constants.DEFAULT_LAST_ORDER_DATE));
              }
              response.setResponseObject(customerDetailsResponse);
              response.setStatus(ResponseStatusCode.SUCCESS);
            } catch (Exception e) {
              log.info(
                  "Error while executing last order date query for botRef:{}, having shopify_customer_id: {}",
                  botRef, customerSetResponseDTO.getCustomerId().toString(), e);
              response.setResponseObject(null);
              response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
              return response;
            }
          } else {
            log.info("Empty customer Object found while requesting customer details for botRef: {}, customerDetailsRequest: {}", botRef, customerDetailsRequest);
            response.setResponseObject(null);
            response.setStatus(ResponseStatusCode.SUCCESS);
            }
          }
        }
      } catch(Exception e){
        log.error(
            "Error while getting Customer Details for: botRef:{}, having customerDetailsRequest:{}",
            botRef, customerDetailsRequest, e);
        response.setResponseObject(null);
        response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      }
    return response;
  }

  private CustomerSetResponseDTO getCustomerId(CustomerDetailsRequest customerDetailsRequest, Long botRef) {
    log.info("Got call to get customerId for botRef: {} having requestBody: {}", botRef,
        customerDetailsRequest);
    CustomerSetResponseDTO customerSetResponseDTO = new CustomerSetResponseDTO();
    Long customerId = 0L;
    try {
      String query = NativeQueries.GET_CUSTOMER_ID_FROM_EMAIL_PHONE;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      if (customerDetailsRequest.getCustomerEmail() != null || !customerDetailsRequest.getCustomerEmail()
          .equals("")) {
        query = query.replace(Constants.EMAIL_PROVIDED, customerDetailsRequest.getCustomerEmail());
      } else {
        query = query.replace(Constants.CUSTOMER_EMAIL_COMPARATOR, "");
      }
      if (customerDetailsRequest.getCustomerPhone() != null || !customerDetailsRequest.getCustomerPhone()
          .equals("")) {
        query = query.replace(Constants.PHONE_PROVIDED, customerDetailsRequest.getCustomerPhone());
      } else {
        query = query.replace(Constants.CUSTOMER_PHONE_NUMBER_COMPARATOR, "");
      }
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        if ((MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class).get(
            Constants.RESPONSE_OBJECT).get(QueryConstants.CUSTOMER_ID).size()) > 0) {
          customerId = Long.valueOf(
              (MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class).get(Constants.RESPONSE_OBJECT).get(QueryConstants.CUSTOMER_ID)).get(0)
                  .toString());
          customerSetResponseDTO.setCustomerId(customerId);
          customerSetResponseDTO.setStatus(String.valueOf(ResponseStatusCode.SUCCESS));
        }else{
          log.info("No matching CustomerId for: botRef:{} and customDetailRequest: {}", botRef, customerDetailsRequest);
          customerSetResponseDTO.setCustomerId(0L);
          customerSetResponseDTO.setStatus(String.valueOf(ResponseStatusCode.SUCCESS));
        }
      }else{
        log.info("Empty response for: botRef:{} and customDetailRequest: {}", botRef, customerDetailsRequest);
        customerSetResponseDTO.setCustomerId(0L);
        customerSetResponseDTO.setStatus(String.valueOf(ResponseStatusCode.SUCCESS));
      }
    } catch (Exception e) {
      log.error("Error while getting CustomerId for: botRef:{} and customDetailRequest: {}", botRef, customerDetailsRequest, e);
      customerSetResponseDTO.setCustomerId(customerId);
      customerSetResponseDTO.setStatus(String.valueOf(ResponseStatusCode.PROCESSING_ERROR));
      return customerSetResponseDTO;
    }
    return customerSetResponseDTO;
  }
}

