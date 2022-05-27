package com.engati.data.analytics.engine.consumer;

import com.engati.data.analytics.engine.constants.constant.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AnalyticsDataConsumer {

  @KafkaListener(id = KafkaConstants.GROUP_ID_ANALYTICS_DATA_REQUEST,
                 topics = {"${topic.shopify.analytics}"},
                 containerFactory = KafkaConstants.CONTAINER_FACTORY)
  public void analyticsRequestConsumed(String payload) {
    try {
      log.info("Payload received from shopify analytics topic is {}", payload);
    } catch (Exception e) {
      log.error("Error while consuming shopify analytics topic", e);
    }
  }

}
