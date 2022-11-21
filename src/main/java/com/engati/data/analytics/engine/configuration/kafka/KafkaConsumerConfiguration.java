package com.engati.data.analytics.engine.configuration.kafka;

import com.engati.data.analytics.engine.constants.constant.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@EnableTransactionManagement
public class KafkaConsumerConfiguration {
  @Value("${spring.kafka.bootstrap.servers}")
  private String bootStrapServerAddress;

  @Value("${kafka.session.timeout.ms:15000}")
  private String consumerSessionTimeout = "15000";

  @Value("${kafka.auto.offset.reset:latest}")
  private String offsetResetConfig = "earliest";

  @Value("${kafka.auto.commit.interval.ms:1000}")
  private String offsetAutoCommitInterval = "1000";

  @Value("${spring.kafka.realtime.concurrency:1}")
  private Integer concurrency;

  @Value("${spring.kafka.max.poll.records:25}")
  private Integer maxPollRecords;


  ConsumerFactory<String, String> kafkaConsumerFactory(Boolean autoCommit) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServerAddress);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, offsetAutoCommitInterval);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeout);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean(KafkaConstants.CONTAINER_FACTORY)
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(kafkaConsumerFactory(Boolean.TRUE));
    factory.setConcurrency(concurrency);
    return factory;
  }

}
