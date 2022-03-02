package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.StoreSegmentationConfiguration;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;


@Repository("com.engati.broadcast.repository.SegmentationConfigurationStoreRepository")
public interface StoreSegmentationConfigurationRepository extends JpaRepository<StoreSegmentationConfiguration, Long> {

  StoreSegmentationConfiguration findByBotRefAndSegmentName(Long botRef, String segmentName);

  List<StoreSegmentationConfiguration> findBySegmentNameAndBotRefIn(String segmentName, List<Long> botRef);
}
