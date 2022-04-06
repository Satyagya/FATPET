package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.CustomerSegmentationConfiguration;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.transaction.Transactional;
import java.util.List;


@Repository("com.engati.broadcast.repository.SegmentationConfigurationStoreRepository")
public interface CustomerSegmentationConfigurationRepository extends JpaRepository<CustomerSegmentationConfiguration, Long> {

  CustomerSegmentationConfiguration findByBotRefAndSegmentName(Long botRef, String segmentName);

  List<CustomerSegmentationConfiguration> findBySegmentNameAndBotRefIn(String segmentName, List<Long> botRef);

}
