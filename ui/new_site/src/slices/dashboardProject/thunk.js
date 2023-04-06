import { createAsyncThunk } from "@reduxjs/toolkit";
//Include Both Helper File with needed methods
import {
  getAllProjectData as getAllProjectDataApi,
  getMonthProjectData as getMonthProjectDataApi,
  gethalfYearProjectData as gethalfYearProjectDataApi,
  getYearProjectData as getYearProjectDataApi,
  getAllProjectStatusData as getAllProjectStatusDataApi,
  getWeekProjectStatusData as getWeekProjectStatusDataApi,
  getMonthProjectStatusData as getMonthProjectStatusDataApi,
  getQuarterProjectStatusData as getQuarterProjectStatusDataApi
} from "../../helpers/fakebackend_helper";

export const getProjectChartsData = createAsyncThunk("dashboardProject/getProjectChartsData", async (data) => {
  try {
    var response;
    if (data === "all") {
      response = getAllProjectDataApi(data);
    }
    if (data === "month") {
      response = getMonthProjectDataApi(data);
    }
    if (data === "halfyear") {
      response = gethalfYearProjectDataApi(data);
    }
    if (data === "year") {
      response = getYearProjectDataApi(data);
    }
    return response;
  } catch (error) {
    return error;
  }
});

export const getProjectStatusChartsData = createAsyncThunk("dashboardProject/getProjectStatusChartsData", async (data) => {
  try {
    var response;
    if (data === "all") {
      response = getAllProjectStatusDataApi(data);
    }
    if (data === "week") {
      response = getWeekProjectStatusDataApi(data);
    }
    if (data === "month") {
      response = getMonthProjectStatusDataApi(data);
    }
    if (data === "quarter") {
      response = getQuarterProjectStatusDataApi(data);
    }
    return response;
  } catch (error) {
    return error;
  }
});