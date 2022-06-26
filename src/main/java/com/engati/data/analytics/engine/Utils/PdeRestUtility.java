package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.constants.constant.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import net.minidev.json.JSONObject;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;
import retrofit2.http.Path;

public interface PdeRestUtility {

  @POST(value = Constants.PDE_EXECUTE_QUERY)
  Call<JsonNode> getProductDetails(@Path(Constants.BOTREF) Long botRef, @Path(Constants.DOMAIN) String domain,
      @Body JSONObject requestBody);

}
