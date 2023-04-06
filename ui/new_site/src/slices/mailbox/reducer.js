import { createSlice } from "@reduxjs/toolkit";
import { getMailDetails, deleteMail } from './thunk';

export const initialState = {
  mailDetails: [],
  error: {},
  isLoader : false
};

const MailBoxSlice = createSlice({
  name: 'MailBoxSlice',
  initialState,
  reducer: {},
  extraReducers: (builder) => {
    builder.addCase(getMailDetails.fulfilled, (state, action) => {
      state.mailDetails = action.payload;
      state.isLoader = false;
    });
    builder.addCase(getMailDetails.rejected, (state, action) => {
      state.error = action.payload.error || null;
      state.isLoader = true;
    });
    builder.addCase(deleteMail.fulfilled, (state, action) => {
      state.mailDetails = state.mailDetails.filter(
        mail => mail.forId !== action.payload
      );
      state.isMailDetailsDeleted = false;
    });
    builder.addCase(deleteMail.rejected, (state, action) => {
      state.error = action.payload.error || null;
      state.isMailDetailsDeleted = false;
    });
  }
});

export default MailBoxSlice.reducer;