package com.engati.data.analytics.engine.configuration.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfiguration {

  @Value("${spring.kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.ack}")
  private String ack;

  @Value("${spring.kafka.retries}")
  private String retries;

  @Value("${spring.kafka.linger.ms}")
  private String linger;

  @Value("${spring.kafka.buffer.memory}")
  private String bufferMemory;

  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, ack);
    props.put(ProducerConfig.RETRIES_CONFIG, retries);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
    return props;
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

}

