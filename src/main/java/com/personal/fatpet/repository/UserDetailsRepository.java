package com.personal.fatpet.repository;

import com.personal.fatpet.entity.UserDetailsConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserDetailsRepository extends JpaRepository<UserDetailsConfig, Long> {

}
