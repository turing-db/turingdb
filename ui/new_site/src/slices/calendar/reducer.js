import { createSlice } from "@reduxjs/toolkit";

import { getEvents, addNewEvent, updateEvent, deleteEvent, getCategories, getUpCommingEvent ,resetCalendar} from './thunk';

export const initialState = {
  events: [],
  categories: [],
  upcommingevents: [],
  error: {}
};


const calendarSlice = createSlice({
  name: "calendar",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(getEvents.fulfilled, (state, action) => {
      state.events = action.payload;
    });
    builder.addCase(getEvents.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(addNewEvent.fulfilled, (state, action) => {
      state.events.push(action.payload);
    });
    builder.addCase(addNewEvent.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(updateEvent.fulfilled, (state, action) => {
      state.events = (state.events || []).map((event) =>
        event.id.toString() === action.payload.id.toString()
          ? { ...event, ...action.payload }
          : event
      );
    });

    builder.addCase(updateEvent.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(deleteEvent.fulfilled, (state, action) => {
      state.events = state.events.filter(
        (event) => event.id.toString() !== action.payload.id.toString()
      );
    });
    builder.addCase(deleteEvent.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getCategories.fulfilled, (state, action) => {
      state.categories = action.payload;
    });
    builder.addCase(getCategories.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getUpCommingEvent.fulfilled, (state, action) => {
      state.upcommingevents = action.payload;
    });

    builder.addCase(getUpCommingEvent.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(resetCalendar.fulfilled, (state, action) => {
      var flag = action.payload.flag;
      var value = action.payload.value;
      var flags = {};
      flags[flag] = value;

      // state.flags = action.payload;
    });

    builder.addCase(resetCalendar.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

  },
});

export default calendarSlice.reducer;