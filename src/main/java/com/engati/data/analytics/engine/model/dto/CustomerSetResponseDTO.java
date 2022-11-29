package com.engati.data.analytics.engine.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomerSetResponseDTO {
  Long customerId;
  String status;

}
