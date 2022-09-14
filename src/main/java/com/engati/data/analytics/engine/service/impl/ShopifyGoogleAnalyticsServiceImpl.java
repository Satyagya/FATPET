package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.TableConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.entity.ShopifyGoogleAnalyticsInfo;
import com.engati.data.analytics.engine.repository.ShopifyGoogleAnalyticsInfoRepository;
import com.engati.data.analytics.engine.service.PrometheusManagementService;
import com.engati.data.analytics.engine.service.ShopifyGoogleAnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

@Service("com.engati.data.analytics.engine.service.ShopifyGoogleAnalyticsService")
@Slf4j
public class ShopifyGoogleAnalyticsServiceImpl implements ShopifyGoogleAnalyticsService {

  @Autowired
  private ShopifyGoogleAnalyticsInfoRepository shopifyGoogleAnalyticsInfoRepository;

  @Autowired
  @Qualifier("com.engati.data.analytics.engine.service.impl.PrometheusManagementServiceImpl")
  private PrometheusManagementService prometheusManagementService;

  @Override
  public DataAnalyticsResponse<String> manageGACreds(MultipartFile authJson, Integer botRef,
      Integer propertyId) {

    DataAnalyticsResponse<String> response =
        new DataAnalyticsResponse<>(ResponseStatusCode.PROCESSING_ERROR);

    try {
      if (Objects.equals(authJson.getContentType(), null)) {
        log.info("Received request to delete ga creds for botRef {}", botRef);
        shopifyGoogleAnalyticsInfoRepository.deleteByBotRef(botRef);
        response.setStatus(ResponseStatusCode.SUCCESS);
      } else {
        if (Objects.equals(authJson.getContentType(), TableConstants.JSON_FILE_TYPE)
            && Objects.nonNull(propertyId)) {
          log.info("Processing request to store GA Creds for botRef {}", botRef);
          JSONParser parser = new JSONParser();
          InputStream inputStream = authJson.getInputStream();
          JSONObject jsonObject = CommonUtils.removeUnnecessaryKeys(
              (JSONObject) parser.parse(new BufferedReader(new InputStreamReader(inputStream))));
          inputStream.close();
          if (CommonUtils.validateAuthJsonFile(jsonObject)) {
            ShopifyGoogleAnalyticsInfo shopifyGoogleAnalyticsInfo =
                ShopifyGoogleAnalyticsInfo.builder().botRef(botRef)
                    .credentials(jsonObject.toJSONString()).propertyId(propertyId).build();
            log.info("Storing GA Creds into db for botRef {}", botRef);
            shopifyGoogleAnalyticsInfoRepository.save(shopifyGoogleAnalyticsInfo);
            response.setStatus(ResponseStatusCode.SUCCESS);
          } else {
            response.setStatus(ResponseStatusCode.INVALID_AUTH_JSON_FILE);
          }
        } else {
          response.setStatus(ResponseStatusCode.INVALID_FILE_TYPE_OR_PROPERTY_ID);
        }
      }
    } catch (ParseException e) {
      prometheusManagementService.apiRequestFailureEvent("manageGACreds", 0L, e.getMessage(),
          StringUtils.EMPTY);
      log.error("Error while processing the file for botRef {}", botRef, e);
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("manageGACreds", 0L, e.getMessage(),
          StringUtils.EMPTY);
      log.error("Error while processing the request for botRef {}", botRef, e);
    }
    return response;
  }

}
