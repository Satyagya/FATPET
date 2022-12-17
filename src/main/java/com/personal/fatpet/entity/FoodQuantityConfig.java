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

@Data
@ToString(doNotUseGetters = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = FatPetConstants.FOOD_QUANTITY_CONFIGURATION)
public class FoodQuantityConfig {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Column(name = "FOOD_NAME")
  private String foodName;

  @Column(name = "ANIMAL_SPECIES")
  private String animalSpecies;

  @Column(name = "TIME_PER_FIFTY_GRAM")
  private String timePerFiftyGrame;

  @Column(name = "MOTOR_ROTAION_ANGLE")
  private String motorRotationAngle;

  @Column(name = "CALORIES_PER_FIFTY_GRAM")
  private int caloriesPerFiftyGram;
}
