package com.engati.data.analytics.engine.druid.query.service.impl;

import com.engati.data.analytics.engine.druid.query.druidry.postAggregator.FinalizingFieldAccessPostAggregator;
import com.engati.data.analytics.engine.druid.query.service.DruidPostAggregateGenerator;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import in.zapr.druid.druidry.postAggregator.ArithmeticFunction;
import in.zapr.druid.druidry.postAggregator.ArithmeticPostAggregator;
import in.zapr.druid.druidry.postAggregator.ConstantPostAggregator;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.postAggregator.FieldAccessPostAggregator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class DruidPostAggregateGeneratorImpl implements DruidPostAggregateGenerator {

  @Override
  public List<DruidPostAggregator> getQueryPostAggregators(List<DruidPostAggregatorMetaInfo>
      postAggregateMetaInfoDtos, Integer botRef, Integer customerId) {

    log.debug("DruidPostAggregateGeneratorImpl: Generating druid post-aggregator from the "
        + "meta-info: {} for botRef: {} and customerId: {}", postAggregateMetaInfoDtos,
        botRef, customerId);
    List<DruidPostAggregator> druidPostAggregatorList = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(postAggregateMetaInfoDtos)) {
      for (DruidPostAggregatorMetaInfo postAggregateMetaInfoDto : postAggregateMetaInfoDtos) {
        DruidPostAggregator postAggregator =
            getPostAggregatorRespectToType(postAggregateMetaInfoDto, botRef, customerId);
        if (Objects.nonNull(postAggregator)) {
          druidPostAggregatorList.add(postAggregator);
        }
      }
    }
    return druidPostAggregatorList;
  }

  private DruidPostAggregator getPostAggregatorRespectToType(DruidPostAggregatorMetaInfo
      postAggregateMetaInfoDto, Integer botRef, Integer customerId) {
    DruidPostAggregator druidPostAggregator = null;
    switch(postAggregateMetaInfoDto.getType()) {
      case ARITHMETIC:
        druidPostAggregator = getArithmeticPostAggregator(postAggregateMetaInfoDto,
            botRef, customerId);
        break;
      case FIELD_ACCESSOR:
        druidPostAggregator = getFieldAccessPostAggregator(postAggregateMetaInfoDto,
            botRef, customerId);
        break;
      case CONSTANT:
        druidPostAggregator = getConstantPostAggregator(postAggregateMetaInfoDto,
            botRef, customerId);
        break;
      case FINALIZING_FIELD_ACCESS:
        druidPostAggregator = getFinalizingFieldAccessPostAggregator(postAggregateMetaInfoDto,
            botRef, customerId);
        break;
      default:
        log.error("DruidPostAggregateGeneratorImpl: Provided postAggregator type: {} does not "
                + "have implementation for botRef: {}, customerId: {}",
            postAggregateMetaInfoDto, botRef, customerId);
    }
    return druidPostAggregator;
  }

  private ArithmeticPostAggregator getArithmeticPostAggregator(DruidPostAggregatorMetaInfo
      postAggregateMetaInfoDto, Integer botRef, Integer customerId) {

    List<DruidPostAggregator> fieldsList = new ArrayList<>();
    for (DruidPostAggregatorMetaInfo postAggregateMetaInfo: postAggregateMetaInfoDto.getFields()) {
      fieldsList.add(getPostAggregatorRespectToType(postAggregateMetaInfo, botRef, customerId));
    }
    ArithmeticPostAggregator arithmeticPostAggregator = ArithmeticPostAggregator.builder()
        .name(postAggregateMetaInfoDto.getPostAggregatorName()).fields(fieldsList)
        .function(ArithmeticFunction.valueOf(postAggregateMetaInfoDto.getFunction()))
        .build();
    return arithmeticPostAggregator;
  }

  private FieldAccessPostAggregator getFieldAccessPostAggregator(DruidPostAggregatorMetaInfo
      druidPostAggregateMetaInfoDto, Integer botRef, Integer customerId) {
    return new FieldAccessPostAggregator(druidPostAggregateMetaInfoDto.getPostAggregatorName(),
        druidPostAggregateMetaInfoDto.getFieldName());
  }

  private ConstantPostAggregator getConstantPostAggregator(DruidPostAggregatorMetaInfo
      druidPostAggregateMetaInfoDto, Integer botRef, Integer customerId) {
    return new ConstantPostAggregator(druidPostAggregateMetaInfoDto.getPostAggregatorName(),
        druidPostAggregateMetaInfoDto.getValue());
  }

  private FinalizingFieldAccessPostAggregator getFinalizingFieldAccessPostAggregator(
      DruidPostAggregatorMetaInfo druidPostAggregateMetaInfoDto, Integer botRef,
      Integer customerId) {
    return new FinalizingFieldAccessPostAggregator(druidPostAggregateMetaInfoDto
        .getPostAggregatorName(), druidPostAggregateMetaInfoDto.getFieldName());
  }
}
