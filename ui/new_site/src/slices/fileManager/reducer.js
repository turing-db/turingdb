import { createSlice } from "@reduxjs/toolkit";
import { getFolders, addNewFolder, updateFolder, deleteFolder, getFiles, addNewFile, updateFile, deleteFile } from './thunk';
export const initialState = {
  folders: [],
  files: [],
  error: {},
};

const FileManagerSlice = createSlice({
  name: 'FileManagerSlice',
  initialState,
  reducer: {},
  extraReducers: (builder) => {
    builder.addCase(getFolders.fulfilled, (state, action) => {
      state.folders = action.payload;
    });
    builder.addCase(getFolders.rejected, (state, action) => {
      state.error = action.payload;
    });

    builder.addCase(addNewFolder.fulfilled, (state, action) => {
      state.folders.push(action.payload);
    });
    builder.addCase(addNewFolder.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(updateFolder.fulfilled, (state, action) => {
      state.folders = state.folders.map(folder =>
        folder.id.toString() === action.payload.id.toString()
          ? { ...folder, ...action.payload }
          : folder
      );
    });

    builder.addCase(updateFolder.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(deleteFolder.fulfilled, (state, action) => {
      state.folders = state.folders.filter(
        folder => folder.id.toString() !== action.payload.id.toString()
      );
    });
    builder.addCase(deleteFolder.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getFiles.fulfilled, (state, action) => {
      state.files = action.payload;
    });
    builder.addCase(getFiles.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(addNewFile.fulfilled, (state, action) => {
      state.files.push(action.payload);
    });

    builder.addCase(addNewFile.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(updateFile.fulfilled, (state, action) => {
      state.files = state.files.map(file =>
        file.id.toString() === action.payload.id.toString()
          ? { ...file, ...action.payload }
          : file
      );
    });

    builder.addCase(updateFile.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(deleteFile.fulfilled, (state, action) => {
      state.files = state.files.filter(
        file => file.id.toString() !== action.payload.id.toString()
      );
    });
    builder.addCase(deleteFile.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
  }
});

export default FileManagerSlice.reducer;