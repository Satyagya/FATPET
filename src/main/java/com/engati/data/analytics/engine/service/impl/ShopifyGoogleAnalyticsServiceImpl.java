package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.TableConstants;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.entity.ShopifyGoogleAnalyticsInfo;
import com.engati.data.analytics.engine.repository.ShopifyGoogleAnalyticsInfoRepository;
import com.engati.data.analytics.engine.service.ShopifyGoogleAnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Override
  public DataAnalyticsResponse<String> storeGACreds(MultipartFile authJson, Integer botRef,
      Integer propertyId) {

    DataAnalyticsResponse<String> response =
        new DataAnalyticsResponse<>(ResponseStatusCode.PROCESSING_ERROR);

    try {
      if (Objects.equals(authJson.getContentType(), TableConstants.JSON_FILE_TYPE)) {
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
        response.setStatus(ResponseStatusCode.INVALID_FILE_TYPE);
      }
    } catch (ParseException e) {
      log.error("Error while processing the file for botRef {}", botRef, e);
    } catch (Exception e) {
      log.error("Error while processing the request for botRef {}", botRef, e);
    }
    return response;
  }

}
