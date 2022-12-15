package com.personal.fatpet.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.personal.fatpet.constant.FatPetConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@ToString(doNotUseGetters = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = FatPetConstants.FAT_PET_CONFIGURATION)
public class FatPetUserConfig {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "USER_ID")
  private String userId;

  @Column(name = "TIME")
  private String time;

  @Column(name = "QUANTITY_OF_FOOD")
  private String quantityOfFood;

  @Column(name = "AUDIO_FILE_FOR_PET")
  private String audioFileForPet;

  @Column(name = "AUDIO_FILE_FOR_OWNER")
  private String audioFileForOwner;

  @Column(name = "UPDATED_ON")
  @UpdateTimestamp
  private Date updatedOn;
}
