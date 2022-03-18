package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.common.model.SegmentConfigResponse;
import com.engati.data.analytics.engine.constants.Constants;
import com.engati.data.analytics.engine.constants.ResponseStatusCode;
import com.engati.data.analytics.engine.entity.StoreSegmentationConfiguration;
import com.engati.data.analytics.engine.model.request.SegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;
import com.engati.data.analytics.engine.repository.StoreSegmentationConfigurationRepository;
import com.engati.data.analytics.engine.service.StoreSegmentationConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service("com.engati.data.analytics.engine.service.SegmentationConfigurationStoreService")
public class StoreSegmentationConfigurationServiceImpl implements StoreSegmentationConfigurationService {

  @Autowired
  private StoreSegmentationConfigurationRepository storeSegmentationConfigurationRepository;

  @Override
  public SegmentConfigResponse<SegmentationConfigurationResponse> getConfigByBotRefAndSegment(Long customerId, Long botRef, String segmentName) {
    SegmentConfigResponse<SegmentationConfigurationResponse> response = new SegmentConfigResponse<>();
    SegmentationConfigurationResponse segmentationConfigurationResponse = new SegmentationConfigurationResponse();
    log.info("Trying to get config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    try {
      if (customerId != null && botRef != null && StringUtils.isNotBlank(segmentName)){
        List<Long> botRefList = new ArrayList<>();
        botRefList.add(botRef);
        botRefList.add(Constants.DEFAULT_BOTREF);
        List<StoreSegmentationConfiguration> storeSegmentationConfiguration = storeSegmentationConfigurationRepository
            .findBySegmentNameAndBotRefIn(segmentName, botRefList);
        if (storeSegmentationConfiguration.stream().anyMatch(p -> p.getBotRef().longValue() == botRef)) {
         Optional<StoreSegmentationConfiguration> filteredStoreSegmentationConfiguration =
              storeSegmentationConfiguration.stream().filter(p -> p.getBotRef().longValue() == botRef).findFirst();
         if(filteredStoreSegmentationConfiguration.isPresent()){
           BeanUtils.copyProperties(segmentationConfigurationResponse,filteredStoreSegmentationConfiguration.get());
         }
          log.info(filteredStoreSegmentationConfiguration.toString());
        }
        else {
          Optional<StoreSegmentationConfiguration> filteredStoreSegmentationConfiguration =
              storeSegmentationConfiguration.stream().filter(p -> p.getBotRef().longValue() == Constants.DEFAULT_BOTREF).findFirst();
          BeanUtils.copyProperties(segmentationConfigurationResponse, filteredStoreSegmentationConfiguration.get());
        }
        if(segmentationConfigurationResponse.getBotRef().longValue() == Constants.DEFAULT_BOTREF){
          segmentationConfigurationResponse.setBotRef(botRef);
          segmentationConfigurationResponse.setCustomerId(customerId);
       }
        response.setResponseObject(segmentationConfigurationResponse);
        response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
     }
      else {
        response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      }
    }catch (Exception e) {
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      log.info("Unhandled exception caught while getting config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    }
    return response;
  }

  @Override
  public SegmentConfigResponse<SegmentationConfigurationResponse> updateConfigByBotRefAndSegment
      (Long customerId, Long botRef, String segmentName, SegmentationConfigurationRequest segmentationConfigurationRequest) {
    SegmentConfigResponse<SegmentationConfigurationResponse> response = new SegmentConfigResponse<>();
    SegmentationConfigurationResponse segmentationConfigurationResponse = new SegmentationConfigurationResponse();
    try {
      if (customerId != null && botRef != null && StringUtils.isNotBlank(segmentName)){
        StoreSegmentationConfiguration storeSegmentationConfiguration = new StoreSegmentationConfiguration();
        BeanUtils.copyProperties(storeSegmentationConfiguration, segmentationConfigurationRequest);
        BeanUtils.copyProperties(segmentationConfigurationResponse, segmentationConfigurationRequest);
        storeSegmentationConfigurationRepository.save(storeSegmentationConfiguration);
        response.setResponseObject(segmentationConfigurationResponse);
        response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
      }
    } catch (Exception e) {
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      log.info("Unhandled exception caught while updating config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    }
    return response;
  }
}
