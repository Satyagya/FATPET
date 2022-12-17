package com.personal.fatpet.repository;

import com.personal.fatpet.entity.FoodQuantityConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.lang.NonNull;

import java.util.Optional;

public interface FoodQuantityRepository extends JpaRepository<FoodQuantityConfig, Long> {
  @Query("Select * from FOOD_QUANTITY_CONFIGURATION where FOOD_NAME = :foodName limit 1")
  FoodQuantityConfig findByFoodName(@Param("foodName") String foodName);

}
