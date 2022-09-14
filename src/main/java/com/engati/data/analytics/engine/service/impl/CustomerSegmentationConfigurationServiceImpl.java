package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.enums.RecencyMetric;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.entity.CustomerSegmentationConfiguration;
import com.engati.data.analytics.engine.model.request.CustomerSegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationConfigurationResponse;
import com.engati.data.analytics.engine.repository.CustomerSegmentationConfigurationRepository;
import com.engati.data.analytics.engine.service.CustomerSegmentationConfigurationService;
import com.engati.data.analytics.engine.service.PrometheusManagementService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service("com.engati.data.analytics.engine.service.SegmentationConfigurationStoreService")
public class CustomerSegmentationConfigurationServiceImpl implements CustomerSegmentationConfigurationService {

  @Autowired
  private CustomerSegmentationConfigurationRepository customerSegmentationConfigurationRepository;

  @Autowired
  @Qualifier("com.engati.data.analytics.engine.service.impl.PrometheusManagementServiceImpl")
  private PrometheusManagementService prometheusManagementService;

  @Override
  public DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> getSystemSegmentConfigByBotRefAndSegment(Long botRef, String segmentName) {
    DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> response = new DataAnalyticsResponse<>();
    CustomerSegmentationConfigurationResponse customerSegmentationConfigurationResponse = new CustomerSegmentationConfigurationResponse();
    log.info("Trying to get config for botRef: {}, segment: {}", botRef, segmentName);
    try {
      if (StringUtils.isNotBlank(segmentName)){
        List<Long> botRefList = new ArrayList<>();
        botRefList.add(botRef);
        botRefList.add(Constants.DEFAULT_BOTREF);
        List<CustomerSegmentationConfiguration> configurationList = customerSegmentationConfigurationRepository
            .findBySegmentNameAndBotRefIn(segmentName, botRefList);
        if (configurationList.size() == 2){
          for(CustomerSegmentationConfiguration configuration : configurationList){
            if (configuration.getBotRef() == Constants.DEFAULT_BOTREF)
              configurationList.remove(configuration);
          }
          BeanUtils.copyProperties(customerSegmentationConfigurationResponse,configurationList.get(0));
        }
        else {
          BeanUtils.copyProperties(customerSegmentationConfigurationResponse,configurationList.get(0));
          customerSegmentationConfigurationResponse.setBotRef(botRef);
        }
        response.setResponseObject(customerSegmentationConfigurationResponse);
        response.setStatus(ResponseStatusCode.SUCCESS);
     }
      else {
        response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      }
    }catch (Exception e) {
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      prometheusManagementService.apiRequestFailureEvent("getSystemSegmentConfigByBotRefAndSegment",
          botRef, e.getMessage(), segmentName);
      log.info("Exception caught while getting config for botRef: {}, segment: {}", botRef, segmentName, e);
    }
    return response;
  }

  @Override
  public DataAnalyticsResponse<CustomerSegmentationConfiguration> updateSystemSegmentConfigByBotRefAndSegment
      (Long botRef, String segmentName, CustomerSegmentationConfigurationRequest customerSegmentationConfigurationRequest) {
    DataAnalyticsResponse<CustomerSegmentationConfiguration> response = new DataAnalyticsResponse<>();
    try {
      if (StringUtils.isNotBlank(segmentName)){
        List<Long> botRefList = new ArrayList<>();
        botRefList.add(botRef);
        botRefList.add(Constants.DEFAULT_BOTREF);
        List<CustomerSegmentationConfiguration> configurationList = customerSegmentationConfigurationRepository
            .findBySegmentNameAndBotRefIn(segmentName, botRefList);
        if (configurationList.size() == 2){
          for(CustomerSegmentationConfiguration configuration : configurationList){
            if (configuration.getBotRef() == Constants.DEFAULT_BOTREF)
              configurationList.remove(configuration);
          }
        }
        CustomerSegmentationConfiguration customerSegmentationConfiguration = new CustomerSegmentationConfiguration();
        BeanUtils.copyProperties(customerSegmentationConfiguration, customerSegmentationConfigurationRequest);
        customerSegmentationConfiguration.setId(configurationList.get(0).getId());
        customerSegmentationConfiguration.setBotRef(botRef);
        customerSegmentationConfiguration.setSegmentName(segmentName);
        customerSegmentationConfiguration = customerSegmentationConfigurationRepository.save(customerSegmentationConfiguration);
        response.setResponseObject(customerSegmentationConfiguration);
        response.setStatus(ResponseStatusCode.SUCCESS);
      }
      else {
        response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
        log.error("No SegmentName for updating config for botRef: {}, segment: {}", botRef, segmentName);
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent(
          "updateSystemSegmentConfigByBotRefAndSegment", botRef, e.getMessage(),
          CommonUtils.getStringValueFromObject(customerSegmentationConfigurationRequest));
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Exception caught while updating config for botRef: {}, segment: {}", botRef, segmentName, e);
    }
    return response;
  }
}
