package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.StoreSegmentationConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository("com.engati.broadcast.repository.GenerateSegmentsRepository")
public interface GenerateSegmentsRepository extends JpaRepository<StoreSegmentationConfiguration, Long> {
}
