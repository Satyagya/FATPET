package com.personal.fatpet.repository;

import com.personal.fatpet.entity.DefaultSettingsConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import javax.swing.text.html.Option;

public interface DefaultSettingsRepository extends JpaRepository<DefaultSettingsConfig, Long> {

  @Query("Select * from USER_DEFAULT_SETTINGS where USER_ID = :userId")
  Option<DefaultSettingsConfig> getDefaultSettingsConfigByUserId(@Param("userId") String userId);
}
