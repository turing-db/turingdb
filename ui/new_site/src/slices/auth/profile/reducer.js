import { createSlice } from "@reduxjs/toolkit";

export const initialState = {
  error: "",
  success: "",
  user: {}
};

const ProfileSlice = createSlice({
  name: "Profile",
  initialState,
  reducers: { 
    profileSuccess(state, action) {
      state.success = action.payload.status;
      state.user = action.payload.data
    },
    profileError(state, action) {
        state.error = action.payload
    },
    editProfileChange(state){
      state = { ...state };
    },
    resetProfileFlagChange(state){
      state.success = null
    }
  },
});

export const {
    profileSuccess,
    profileError,
    editProfileChange,
    resetProfileFlagChange
} = ProfileSlice.actions

export default ProfileSlice.reducer;