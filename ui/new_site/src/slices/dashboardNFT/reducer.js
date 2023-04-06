import { createSlice } from "@reduxjs/toolkit";
import { getMarketChartsDatas } from './thunk';
export const initialState = {
  marketplaceData: [],
  error: {}
};

const DashboardNFTSlice = createSlice({
  name: 'DashboardNFT',
  initialState,
  reducer: {},
  extraReducers: (builder) => {
    builder.addCase(getMarketChartsDatas.fulfilled, (state, action) => {
      state.marketplaceData = action.payload;
    });

    builder.addCase(getMarketChartsDatas.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
  }
});

export default DashboardNFTSlice.reducer;