import { createAsyncThunk } from "@reduxjs/toolkit";
//Include Both Helper File with needed methods
import {
  getTodayBalanceData as getTodayBalanceDataApi,
  getLastWeekBalanceData as getLastWeekBalanceDataApi,
  getLastMonthBalanceData as getLastMonthBalanceDataApi,
  getCurrentYearBalanceData as getCurrentYearBalanceDataApi,
  getTodayDealData as getTodayDealDataApi,
  getWeeklyDealData as getWeeklyDealDataApi,
  getMonthlyDealData as getMonthlyDealDataApi,
  getYearlyDealData as getYearlyDealDataApi,
  getOctSalesData as getOctSalesDataApi,
  getNovSalesData as getNovSalesDataApi,
  getDecSalesData as getDecSalesDataApi,
  getJanSalesData as getJanSalesDataApi
}
  from "../../helpers/fakebackend_helper";

export const getBalanceChartsData = createAsyncThunk("dashboardCrm/getBalanceChartsData", async (data) => {
  try {
    var response;
    if (data === "today") {
      response = getTodayBalanceDataApi(data);
    }
    if (data === "lastWeek") {
      response = getLastWeekBalanceDataApi(data);
    }
    if (data === "lastMonth") {
      response = getLastMonthBalanceDataApi(data);
    }
    if (data === "currentYear") {
      response = getCurrentYearBalanceDataApi(data);
    }
    return response;
  } catch (error) {
    return error;
  }
});

export const getDialChartsData = createAsyncThunk("dashboardCrm/getDialChartsData", async (data) => {
  try {
    var response;
    if (data === "today") {
      response = getTodayDealDataApi(data);
    }
    if (data === "weekly") {
      response = getWeeklyDealDataApi(data);
    }
    if (data === "monthly") {
      response = getMonthlyDealDataApi(data);
    }
    if (data === "yearly") {
      response = getYearlyDealDataApi(data);
    }
    return response;
  }
  catch (error) {
    return error;
  }
});

export const getSalesChartsData = createAsyncThunk("dashboardCrm/getSalesChartsData", async (data) => {
  try {
    var response;
    if (data === "oct") {
      response = getOctSalesDataApi(data);
    }
    if (data === "nov") {
      response = getNovSalesDataApi(data);
    }
    if (data === "dec") {
      response = getDecSalesDataApi(data);
    }
    if (data === "jan") {
      response = getJanSalesDataApi(data);
    }
    return response;
  }
  catch (error) {
    return response;
  }
});