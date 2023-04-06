import { createSlice } from "@reduxjs/toolkit";
import { getRevenueChartsData } from './thunk';

export const initialState = {
  revenueData: [],
  error: {}
};

const DashboardSlice = createSlice({
  name: 'Dashboard',
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

export default DashboardSlice.reducer;
