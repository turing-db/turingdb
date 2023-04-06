import { createSlice } from "@reduxjs/toolkit";
import { getAPIKey } from './thunk';

export const initialState = {
    apiKey: [],
    error: {},
};

const APIKeyslice = createSlice({
    name: 'APIKey',
    initialState,
    reducers: {},
    extraReducers: (builder) => {
        builder.addCase(getAPIKey.fulfilled, (state, action) => {
            state.apiKey = action.payload;
        });
        builder.addCase(getAPIKey.rejected, (state, action) => {
            state.error = action.payload.error || null;
        });
    }
});

export default APIKeyslice.reducer;