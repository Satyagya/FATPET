package com.engati.data.analytics.engine.queryGenerator.service.impl;

import com.engati.data.analytics.engine.queryGenerator.service.DruidFilterGenerator;
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
  public DruidFilter getFiltersByType(DruidFilterMetaInfo druidFilterMetaInfoDto) {
    DruidFilter druidFilter = null;
    if (Objects.nonNull(druidFilterMetaInfoDto) &&
        Objects.nonNull(druidFilterMetaInfoDto.getType())) {
      switch (druidFilterMetaInfoDto.getType()) {
        case SELECTOR:
          druidFilter = getSelectorFilter((String) druidFilterMetaInfoDto.getDimension(),
              (String) druidFilterMetaInfoDto.getValue());
          break;
        case IN:
          druidFilter = getInFilter(druidFilterMetaInfoDto);
          break;
        default:
          log.error("The given filter is not supported currently - {}", druidFilterMetaInfoDto);
      }
    }
    return druidFilter;
  }

  private List<DruidFilter> getQueryFilters(List<DruidFilterMetaInfo> druidFilterMetaInfoDtos) {
    List<DruidFilter> druidFilters = new ArrayList<>();
    for (DruidFilterMetaInfo druidFilterMetaInfoDto: druidFilterMetaInfoDtos) {
      druidFilters.add(getFiltersByType(druidFilterMetaInfoDto));
    }
    return druidFilters;
  }

  private SelectorFilter getSelectorFilter(String dimension, String value) {
    return new SelectorFilter(dimension, value);
  }

  private SelectorFilter getSelectorFilter(String dimension, Integer value) {
    return new SelectorFilter(dimension, value);
  }

  private InFilter getInFilter(DruidFilterMetaInfo druidFilterMetaInfoDto) {
    return new InFilter((String) druidFilterMetaInfoDto.getDimension(),
        (List) druidFilterMetaInfoDto.getValue());
  }
}

