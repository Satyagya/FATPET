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

import java.io.File;
import java.io.FileReader;
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
        File authFile = CommonUtils.convertMultiPartToFile(authJson);
        JSONObject jsonObject =
            removeUnnecessaryKeys((JSONObject) parser.parse(new FileReader(authFile)));
        authFile.delete();
        if (validateAuthJsonFile(jsonObject)) {
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

  private JSONObject removeUnnecessaryKeys(JSONObject jsonObject) {
    jsonObject.remove(TableConstants.AUTH_URI);
    jsonObject.remove(TableConstants.TOKEN_URI);
    jsonObject.remove(TableConstants.AUTH_PROVIDER_CERT_URL);
    jsonObject.remove(TableConstants.CLIENT_CERT_URL);
    return jsonObject;
  }

  private Boolean validateAuthJsonFile(JSONObject jsonObject) {
    return jsonObject.containsKey(TableConstants.TYPE) && jsonObject.containsKey(
        TableConstants.PROJECT_ID) && jsonObject.containsKey(TableConstants.PRIVATE_KEY_ID)
        && jsonObject.containsKey(TableConstants.PRIVATE_KEY) && jsonObject.containsKey(
        TableConstants.CLIENT_EMAIL) && jsonObject.containsKey(TableConstants.CLIENT_ID);
  }

}
