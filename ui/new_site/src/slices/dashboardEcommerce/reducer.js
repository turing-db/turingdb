import { createSlice } from "@reduxjs/toolkit";
import { getRevenueChartsData } from './thunk';

export const initialState = {
  revenueData: [],
  error: {}
};

const DashboardEcommerceSlice = createSlice({
  name: 'DashboardEcommerce',
  initialState,
  reducer: {},
  extraReducers: (builder) => {
    builder.addCase(getRevenueChartsData.fulfilled, (state, action) => {
      state.revenueData = action.payload;
    });
    builder.addCase(getRevenueChartsData.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
  }
});

export default DashboardEcommerceSlice.reducer;
