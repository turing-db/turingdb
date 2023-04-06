import { createSlice } from "@reduxjs/toolkit";

export const initialState = {
  forgetSuccessMsg: null,
  forgetError: null,
};

const forgotPasswordSlice = createSlice({
  name: "forgotpwd",
  initialState,
  reducers: {
      userForgetPasswordSuccess(state, action) {
          state.forgetSuccessMsg = action.payload
      },
      userForgetPasswordError(state, action) {
          state.forgetError = action.payload
      },
  },
});

export const {
  userForgetPasswordSuccess,
  userForgetPasswordError
} = forgotPasswordSlice.actions

export default forgotPasswordSlice.reducer;
