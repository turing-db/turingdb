import { createAsyncThunk } from "@reduxjs/toolkit";

//Include Both Helper File with needed methods
import {
  getEvents as getEventsApi,
  addNewEvent as addNewEventApi,
  updateEvent as updateEventApi,
  deleteEvent as deleteEventApi,
  getCategories as getCategoriesApi,
  getUpCommingEvent as getUpCommingEventApi
} from "../../helpers/fakebackend_helper";

export const getEvents = createAsyncThunk("calendar/getEvents", async () => {
  try {
    const response = getEventsApi();
    return response;
  } catch (error) {
    return error;
  }
});

export const addNewEvent = createAsyncThunk("calendar/addNewEvent", async (event) => {
  try {
    const response = addNewEventApi(event);
    return response;
  } catch (error) {
    return error;
  }
});

export const updateEvent = createAsyncThunk("calendar/updateEvent", async (event) => {
  try {
    const response = updateEventApi(event);
    const modifiedevent = await response;
    return modifiedevent;
  } catch (error) {
    return error;
  }
});

export const deleteEvent = createAsyncThunk("calendar/deleteEvent", async (event) => {
  try {
    const response = deleteEventApi(event);
    return response;
  } catch (error) {
    return error;
  }
});

export const getCategories = createAsyncThunk("calendar/getCategories", async () => {
  try{
    const response = getCategoriesApi();
    return response;
  }catch (error) {
    return error;
  }
})

export const getUpCommingEvent = createAsyncThunk("calendar/getUpCommingEvent", async () => {
  try{
    const response = getUpCommingEventApi()
    return response;
  }catch (error) {
    return error;
  }
})

export const resetCalendar = createAsyncThunk("calendar/resetCalendar", async () => {
  try{
    const response = '';
    return response;
  }catch (error) {
    return error;
  }
})