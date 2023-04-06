import { createSlice } from "@reduxjs/toolkit";

export const initialState = {
  user: "",
  error: "",
  loading: false,
  isUserLogout: false,
  errorFlag: false,
};

const loginSlice = createSlice({
  name: "login",
  initialState,
  reducers: {
    apiError(state, action) {
      state.error = action.payload.data;
      state.loading = true;
      state.isUserLogout = false;
      state.errorFlag = true;

    },
    loginSuccess(state, action) {
      // state.user = action.payload
      state.loading = false;
      state.errorFlag = false;
    },
    logoutUserSuccess(state, action) {
      state.isUserLogout = true
    },
    reset_login_flag(state) {
      state.error = null
      state.loading = false;
      state.errorFlag = false;
    }
  },
});

export const {
  apiError,
  loginSuccess,
  logoutUserSuccess,
  reset_login_flag
} = loginSlice.actions

export default loginSlice.reducer;