import { createAsyncThunk } from "@reduxjs/toolkit";
//Include Both Helper File with needed methods
import {
  getAllMarketplaceData as getAllMarketplaceDataApi,
  getMonthMarketplaceData as getMonthMarketplaceDataApi,
  gethalfYearMarketplaceData as gethalfYearMarketplaceDataApi,
  getYearMarketplaceData as getYearMarketplaceDataApi
} from "../../helpers/fakebackend_helper";

export const getMarketChartsDatas = createAsyncThunk("dashboardNFT/getMarketChartsDatas", async (data) => {
  try {
    var response;
    if (data === "all") {
      response = getAllMarketplaceDataApi(data);
    }
    if (data === "month") {
      response = getMonthMarketplaceDataApi(data);
    }
    if (data === "halfyear") {
      response = gethalfYearMarketplaceDataApi(data);
    }
    if (data === "year") {
      response = getYearMarketplaceDataApi(data);
    }
    return response;
  } catch (error) {
    return error;
  }
});