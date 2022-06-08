package com.engati.data.analytics.engine.consumer;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.constants.constant.KafkaConstants;
import com.engati.data.analytics.engine.model.dto.CustomSegmentDTO;
import com.engati.data.analytics.engine.model.dto.SystemSegmentDTO;
import com.engati.data.analytics.engine.model.request.CustomSegmentRequest;
import com.engati.data.analytics.engine.service.impl.SegmentServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class SegmentRequestConsumer {

  @Autowired
  private SegmentServiceImpl segmentService;

  @KafkaListener(id = KafkaConstants.GROUP_ID_SEGMENTATION_REQUEST,
      topics = {"${topic.shopify.segments.request}"},
      containerFactory = KafkaConstants.CONTAINER_FACTORY)
  public void analyticsRequestConsumed(String payload) {
    try {
      log.info("Request received for segmentation with payload: {}", payload);
      if (payload.contains("CUSTOM")) {
        CustomSegmentDTO customSegmentDTO = CommonUtils.MAPPER.readValue(payload, CustomSegmentDTO.class);
        CustomSegmentRequest customSegmentRequest = CustomSegmentRequest.builder().segmentName(customSegmentDTO.getSegmentName()).segmentCondition(customSegmentDTO.getSegmentCondition()).build();
        segmentService.getCustomersForCustomSegment(customSegmentDTO.getBotRef().longValue(), customSegmentRequest);
      } else if (payload.contains("SYSTEM")) {
        SystemSegmentDTO systemSegmentDTO = CommonUtils.MAPPER.readValue(payload, SystemSegmentDTO.class);
        segmentService.getCustomersForSystemSegment(systemSegmentDTO.getBotRef().longValue(), systemSegmentDTO.getSegmentName());
      }
    } catch (Exception e) {
      log.error("Error while consuming segmentation request for payload: {}", payload, e);
    }
  }

}
