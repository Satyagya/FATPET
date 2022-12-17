package com.personal.fatpet.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.personal.fatpet.constant.FatPetConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.File;

@Data
@ToString(doNotUseGetters = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = FatPetConstants.USER_DEFAULT_SETTINGS)
public class DefaultSettingsConfig {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Column(name = "USER_ID")
  private String userId;

  @Column(name = "QUANTITY_OF_FOOD")
  private int quantityOfFood;

  @Column(name = "AUDIO_FILE_FOR_PET")
  private File audioFileForPet;

}
