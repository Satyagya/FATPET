package com.personal.fatpet.repository;

import com.personal.fatpet.entity.UserPetDetails;
import io.reactivex.Single;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PetDetailsRepository extends JpaRepository<UserPetDetails, Long> {
  UserPetDetails findByUserId(String userId);
}
