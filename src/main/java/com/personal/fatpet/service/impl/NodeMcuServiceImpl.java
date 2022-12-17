package com.personal.fatpet.service.impl;

import com.personal.fatpet.entity.FatPetUserConfig;
import com.personal.fatpet.entity.FoodQuantityConfig;
import com.personal.fatpet.entity.UserDetailsConfig;
import com.personal.fatpet.entity.UserPetDetails;
import com.personal.fatpet.model.FatPetResponse;
import com.personal.fatpet.model.FatPetStatusCode;
import com.personal.fatpet.model.UserPetDetailsModel;
import com.personal.fatpet.repository.FoodQuantityRepository;
import com.personal.fatpet.repository.PetDetailsRepository;
import com.personal.fatpet.repository.UserDetailsRepository;
import com.personal.fatpet.service.NodeMcuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.personal.fatpet.constant.FatPetConstants.API_VALIDATION_STRING;
import static com.personal.fatpet.constant.FatPetConstants.WAV_FILE_PATH;

@Slf4j
@Service("com.personal.fatpet.service.impl.NodeMcuServiceImpl")
public class NodeMcuServiceImpl implements NodeMcuService {

  @Autowired
  private UserDetailsRepository userDetailsRepository;

  @Autowired
  private PetDetailsRepository petDetailsRepository;

  @Autowired
  private FoodQuantityRepository foodQuantityRepository;

  @Override
  public FatPetResponse<UserDetailsConfig> saveInitialDetailsAndGetUserId(String userName,
      String ipAddress, String ssid) {
    FatPetResponse<UserDetailsConfig> response = new FatPetResponse<>(FatPetStatusCode.FAIL);
    ipAddress = ipAddress + "/" + API_VALIDATION_STRING;
    UserDetailsConfig userDetailsConfig =
        UserDetailsConfig.builder().userName(userName).ipAddress(ipAddress).ssid(ssid).build();
    try {
      UserDetailsConfig updatedUserDetailsConfig = userDetailsRepository.save(userDetailsConfig);
      if (Objects.nonNull(updatedUserDetailsConfig.getId())) {
        response.setResponseObject(updatedUserDetailsConfig);
        response.setStatus(FatPetStatusCode.SUCCESS);
      }
    } catch (Exception e) {
      response.setStatus(FatPetStatusCode.PROCESSING_ERROR);
      log.error("Encountered an Exception while saving Initial Data:", e);
    }
    return response;
  }

  @Override
  public FatPetResponse<Boolean> savePetDetails(String userId,
      UserPetDetailsModel userPetDetailsModel) {
    FatPetResponse<Boolean> response = new FatPetResponse<>(FatPetStatusCode.FAIL);
    UserPetDetails userPetDetails =
        UserPetDetails.builder().userId(userId).petName(userPetDetailsModel.getPetName())
            .age(userPetDetailsModel.getAge()).breed(userPetDetailsModel.getBreed())
            .weight(userPetDetailsModel.getWeight()).height(userPetDetailsModel.getHeight())
            .specie(userPetDetailsModel.getSpecie())
            .activityLevel(userPetDetailsModel.getActivityLevel())
            .foodName(userPetDetailsModel.getFoodName()).build();
    try {
      petDetailsRepository.save(userPetDetails);
    } catch (Exception e) {
      response.setStatus(FatPetStatusCode.PROCESSING_ERROR);
      log.error("Encountered an Exception while saving Pet's Data:", e);
    }
    return response;
  }

  @Override
  public FatPetResponse<List<FatPetUserConfig>> getRecommendedSettings(String userId) {
    FatPetResponse<List<FatPetUserConfig>> response = new FatPetResponse<>(FatPetStatusCode.FAIL);
    List<FatPetUserConfig> fatPetUserConfigList = new ArrayList<>();
    try {
      UserPetDetails userPetDetails = petDetailsRepository.findByUserId(userId);
      FoodQuantityConfig foodQuantityConfig =
          foodQuantityRepository.findByFoodName(userPetDetails.getFoodName());
      List<LocalTime> timestampsForDispensingFood = getRecommendedTimestampsForFood(userPetDetails);
      int totalQuantityOfFood = getRecommendedQuantityForPet(userPetDetails, foodQuantityConfig);
      int singleQuantityFood = totalQuantityOfFood/(timestampsForDispensingFood.size());
      File defaultAudioForPet = getDefaultAudioForPet(userPetDetails.getSpecie());
      for (LocalTime time : timestampsForDispensingFood) {
        FatPetUserConfig fatPetUserConfig =
            FatPetUserConfig.builder().userId(userId).time(time).quantityOfFood(singleQuantityFood)
                .audioFileForPet(defaultAudioForPet).build();
        fatPetUserConfigList.add(fatPetUserConfig);
      }
      response.setResponseObject(fatPetUserConfigList);
      response.setStatus(FatPetStatusCode.SUCCESS);
    } catch (Exception e) {
      response.setStatus(FatPetStatusCode.PROCESSING_ERROR);
      log.error("Encountered an Exception while getting Recommended Settings:", e);
    }
    return response;
  }

  private File getDefaultAudioForPet(String specie) {
    File file = new File(WAV_FILE_PATH);
    if(Objects.equals(specie, "DOG")) {
      file = new File(WAV_FILE_PATH + "dog.wav");
    }
    else if(Objects.equals(specie, "CAT")) {
      file = new File(WAV_FILE_PATH + "cat.wav");
    }
    return file;
  }

  private int getRecommendedQuantityForPet(UserPetDetails userPetDetails,
      FoodQuantityConfig foodQuantityConfig) {
    int quantity = 100;
    double calc1 = Math.pow(userPetDetails.getWeight(), 0.75);
    double calorieCount = calc1 * 70;
    switch (userPetDetails.getActivityLevel()) {
      case 1:
        calorieCount = calorieCount * 0.7;
        break;
      case 2:
        calorieCount = calorieCount * 0.8;
        break;
      case 3:
        calorieCount = calorieCount * 0.9;
        break;
      case 4:
        calorieCount = calorieCount * 1;
        break;

      case 5:
        calorieCount = calorieCount * 1.2;
        break;

      case 6:
        calorieCount = calorieCount * 1.4;
        break;

      case 7:
        calorieCount = calorieCount * 1.8;
        break;

      case 9:
        calorieCount = calorieCount * 2.6;
        break;

      case 10:
        calorieCount = calorieCount * 3;
        break;
    }
    quantity = (int) ((calorieCount * 50) / foodQuantityConfig.getCaloriesPerFiftyGram());
    return quantity;
  }

  private List<LocalTime> getRecommendedTimestampsForFood(UserPetDetails userPetDetails) {
    List<LocalTime> timestampsForDispensingFood = new ArrayList<>();
    if (Objects.equals(userPetDetails.getSpecie(), "DOG")) {
      timestampsForDispensingFood.add(LocalTime.parse("08:00:00"));
      timestampsForDispensingFood.add(LocalTime.parse("18:00:00"));
    } else if (Objects.equals(userPetDetails.getSpecie(), "CAT")) {
      timestampsForDispensingFood.add(LocalTime.parse("07:00:00"));
      timestampsForDispensingFood.add(LocalTime.parse("15:00:00"));
      timestampsForDispensingFood.add(LocalTime.parse("23:00:00"));
    }
    return timestampsForDispensingFood;
  }

}

//TODO:
/**
 * 1. Check if u can use single api for all the data - NO WE WILL USE MULTIPLE APIS
 * 2. Work on filling the databases (run mysql on local)
 * 3. Fill the food DB
 * 4. Create logic for the food db
 * 5. Send FatPetUserConfig to suggest recommended quantity
 */
