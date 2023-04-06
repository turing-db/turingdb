import { createSlice } from "@reduxjs/toolkit";
import { getTeamData, addTeamData, updateTeamData, deleteTeamData } from './thunk';
export const initialState = {
    teamData: [],
    error: {},
};

const TeamSlice = createSlice({
    name: 'TeamSlice',
    initialState,
    reducers: {},
    extraReducers: (builder) => {
        builder.addCase(getTeamData.fulfilled, (state, action) => {
            state.teamData = action.payload;
        });
        builder.addCase(getTeamData.rejected, (state, action) => {
            state.error = action.payload.error || null;
        });
        builder.addCase(addTeamData.fulfilled, (state, action) => {
            state.teamData.push(action.payload);
        });
        builder.addCase(addTeamData.rejected, (state, action) => {
            state.error = action.payload.error || null;
        });
        builder.addCase(updateTeamData.fulfilled, (state, action) => {
            state.teamData = state.teamData.map(team =>
                team.id.toString() === action.payload.id.toString()
                    ? { ...team, ...action.payload }
                    : team
            );
        });
        builder.addCase(updateTeamData.rejected, (state, action) => {
            state.error = action.payload.error || null;
        });
        builder.addCase(deleteTeamData.fulfilled, (state, action) => {
            state.teamData = state.teamData.filter(team => team.id.toString() !== action.payload.id.toString());
        });
        builder.addCase(deleteTeamData.rejected, (state, action) => {
            state.error = action.payload.error || null;
        });
    }
});

export default TeamSlice.reducer;