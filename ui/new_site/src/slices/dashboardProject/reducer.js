import { createSlice } from "@reduxjs/toolkit";
import { getProjectChartsData, getProjectStatusChartsData } from './thunk';

export const initialState = {
  projectData: [],
  projectStatusData: [],
  error: {}
};


const DashboardProjectSlice = createSlice({
  name: 'DashboardProject',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(getProjectChartsData.fulfilled, (state, action) => {
      state.projectData = action.payload;
    });
    builder.addCase(getProjectChartsData.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getProjectStatusChartsData.fulfilled, (state, action) => {
      state.projectStatusData = action.payload;
    });
    builder.addCase(getProjectStatusChartsData.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
  }
});

export default DashboardProjectSlice.reducer;