package com.engati.data.analytics.engine.druid.query.service.impl;

import com.engati.data.analytics.engine.druid.query.service.DruidFilterGenerator;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.filter.InFilter;
import in.zapr.druid.druidry.filter.SelectorFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class DruidFilterGeneratorImpl implements DruidFilterGenerator {

  @Override
  public DruidFilter getFiltersByType(DruidFilterMetaInfo druidFilterMetaInfoDto,
      Integer botRef, Integer customerId) {
    DruidFilter druidFilter = null;
    if (Objects.nonNull(druidFilterMetaInfoDto) &&
        Objects.nonNull(druidFilterMetaInfoDto.getType())) {
      switch (druidFilterMetaInfoDto.getType()) {
        case SELECTOR:
          druidFilter = getSelectorFilter((String) druidFilterMetaInfoDto.getDimension(),
              (String) druidFilterMetaInfoDto.getValue(), botRef, customerId);
          break;
        case IN:
          druidFilter = getInFilter(druidFilterMetaInfoDto, botRef, customerId);
          break;
        default:
          log.error("DruidFilterGeneratorImpl: Provided filter type: {} does not "
              + "have implementation for botRef: {}, customerId: {}",
              druidFilterMetaInfoDto.getType(), botRef, customerId);
      }
    }
    return druidFilter;
  }

  private List<DruidFilter> getQueryFilters(List<DruidFilterMetaInfo> druidFilterMetaInfoDtos,
      Integer botRef, Integer customerId) {
    List<DruidFilter> druidFilters = new ArrayList<>();
    for (DruidFilterMetaInfo druidFilterMetaInfoDto: druidFilterMetaInfoDtos) {
      druidFilters.add(getFiltersByType(druidFilterMetaInfoDto, botRef, customerId));
    }
    return druidFilters;
  }

  private SelectorFilter getSelectorFilter(String dimension, String value, Integer botRef,
      Integer customerId) {
    return new SelectorFilter(dimension, value);
  }

  private SelectorFilter getSelectorFilter(String dimension, Integer value,
      Integer botRef, Integer customerId) {
    return new SelectorFilter(dimension, value);
  }

  private InFilter getInFilter(DruidFilterMetaInfo druidFilterMetaInfoDto,
      Integer botRef, Integer customerId) {
    return new InFilter((String) druidFilterMetaInfoDto.getDimension(),
        (List) druidFilterMetaInfoDto.getValue());
  }
}

