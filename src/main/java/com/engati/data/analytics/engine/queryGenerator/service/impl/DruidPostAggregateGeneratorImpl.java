package com.engati.data.analytics.engine.queryGenerator.service.impl;

import com.engati.data.analytics.engine.queryGenerator.druidry.postAggregator.FinalizingFieldAccessPostAggregator;
import com.engati.data.analytics.engine.queryGenerator.service.DruidPostAggregateGenerator;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import in.zapr.druid.druidry.postAggregator.ArithmeticPostAggregator;
import in.zapr.druid.druidry.postAggregator.ConstantPostAggregator;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.postAggregator.FieldAccessPostAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class DruidPostAggregateGeneratorImpl implements DruidPostAggregateGenerator {

  @Override
  public List<DruidPostAggregator> getQueryPostAggregators(
      List<DruidPostAggregatorMetaInfo> postAggregateMetaInfoDtos) {

    List<DruidPostAggregator> druidPostAggregatorList = new ArrayList<>();
    for (DruidPostAggregatorMetaInfo postAggregateMetaInfoDto: postAggregateMetaInfoDtos) {
      druidPostAggregatorList.add(getPostAggregatorRespectToType(postAggregateMetaInfoDto));
    }
    return druidPostAggregatorList;
  }

  private DruidPostAggregator getPostAggregatorRespectToType(DruidPostAggregatorMetaInfo
      postAggregateMetaInfoDto) {
    DruidPostAggregator druidPostAggregator = null;
    switch(postAggregateMetaInfoDto.getType()) {
      case ARITHMETIC:
        druidPostAggregator = getArithmeticPostAggregator(postAggregateMetaInfoDto);
        break;
      case FIELD_ACCESSOR:
        druidPostAggregator = getFieldAccessPostAggregator(postAggregateMetaInfoDto);
        break;
      case CONSTANT:
        druidPostAggregator = getConstantPostAggregator(postAggregateMetaInfoDto);
        break;
      case FINALIZING_FIELD_ACCESS:
        druidPostAggregator = getFinalizingFieldAccessPostAggregator(postAggregateMetaInfoDto);
        break;
      default:
        log.error("The given type in postAggregator is not supported: {}",
            postAggregateMetaInfoDto);
    }
    return druidPostAggregator;
  }

  private ArithmeticPostAggregator getArithmeticPostAggregator(DruidPostAggregatorMetaInfo
      postAggregateMetaInfoDto) {

    List<DruidPostAggregator> fieldsList = new ArrayList<>();
    for (DruidPostAggregatorMetaInfo postAggregateMetaInfo: postAggregateMetaInfoDto.getFields()) {
      fieldsList.add(getPostAggregatorRespectToType(postAggregateMetaInfo));
    }
    //todo function to get druid arithmetic function
    ArithmeticPostAggregator arithmeticPostAggregator = ArithmeticPostAggregator.builder()
        .name(postAggregateMetaInfoDto.getName()).fields(fieldsList)
//        .function(postAggregateMetaInfoDto.getFunction())
        .build();
    return arithmeticPostAggregator;
  }

  private FieldAccessPostAggregator getFieldAccessPostAggregator(DruidPostAggregatorMetaInfo
      druidPostAggregateMetaInfoDto) {
    return new FieldAccessPostAggregator(druidPostAggregateMetaInfoDto.getName(),
        druidPostAggregateMetaInfoDto.getFieldName());
  }

  private ConstantPostAggregator getConstantPostAggregator(DruidPostAggregatorMetaInfo
      druidPostAggregateMetaInfoDto) {
    return new ConstantPostAggregator(druidPostAggregateMetaInfoDto.getName(),
        druidPostAggregateMetaInfoDto.getValue());
  }

  private FinalizingFieldAccessPostAggregator getFinalizingFieldAccessPostAggregator(
      DruidPostAggregatorMetaInfo druidPostAggregateMetaInfoDto) {
    return new FinalizingFieldAccessPostAggregator(druidPostAggregateMetaInfoDto.getName(),
        druidPostAggregateMetaInfoDto.getFieldName());
  }
}
