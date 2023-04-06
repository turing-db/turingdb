import { createAsyncThunk } from "@reduxjs/toolkit";
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

//Include Both Helper File with needed methods
import {
  getFolders as getFoldersApi,
  addNewFolder as addNewFolderApi,
  updateFolder as updateFolderApi,
  deleteFolder as deleteFolderApi,
  getFiles as getFilesApi,
  addNewFile as addNewFileApi,
  updateFile as updateFileApi,
  deleteFile as deleteFileApi,
} from "../../helpers/fakebackend_helper";

export const getFolders = createAsyncThunk("fileManager/getFolders", async () => {
  try {
    const response = getFoldersApi();
    return response;
  }
  catch (error) {
    return error;
  }
});

export const addNewFolder = createAsyncThunk("fileManager/addNewFolder", async (folder) => {
  try {
    const response = addNewFolderApi(folder);
    toast.success("Folder Added Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("Folder Added Failed", { autoClose: 3000 });
    return error;
  }
});

export const updateFolder = createAsyncThunk("fileManager/updateFolder", async (folder) => {
  try {
    const response = updateFolderApi(folder);
    toast.success("Folder Updated Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("Folder Updated Failed", { autoClose: 3000 });
    return error;
  }
});

export const deleteFolder = createAsyncThunk("fileManager/deleteFolder", async (folder) => {
  try {
    const response = deleteFolderApi(folder);
    toast.success("Order Deleted Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("Order Deleted Failed", { autoClose: 3000 });
    return error;
  }
});

export const getFiles = createAsyncThunk("fileManager/getFiles", async () => {
  try {
    const response = getFilesApi();
    return response;
  } catch (error) {
    return error;
  }
});

export const addNewFile = createAsyncThunk("fileManager/addNewFile", async (file) => {
  try {
    const response = addNewFileApi(file);
    toast.success("File Added Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("File Added Failed", { autoClose: 3000 });
    return error;
  }
});

export const updateFile = createAsyncThunk("fileManager/updateFile", async (file) => {
  try {
    const response = updateFileApi(file);
    toast.success("File Updated Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("File Updated Failed", { autoClose: 3000 });
    return error;
  }
});

export const deleteFile = createAsyncThunk("fileManager/deleteFile", async (file) => {
  try {
    const response = deleteFileApi(file);
    toast.success("File Delete Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("File Delete Failed", { autoClose: 3000 });
    return error;
  }
});