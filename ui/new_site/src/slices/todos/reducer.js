import { createSlice } from "@reduxjs/toolkit";
import { getTodos, addNewTodo, updateTodo, deleteTodo, getProjects, addNewProject } from './thunk';
export const initialState = {
  todos: [],
  projects: [],
  error: {},
};

const TodosSlice = createSlice({
  name: 'TodosSlice',
  initialState,
  reducer: {},
  extraReducers: (builder) => {
    builder.addCase(getTodos.fulfilled, (state, action) => {
      state.todos = action.payload;
    });
    builder.addCase(getTodos.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
    builder.addCase(addNewTodo.fulfilled, (state, action) => {
      state.todos.push(action.payload);
    });
    builder.addCase(addNewTodo.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
    builder.addCase(updateTodo.fulfilled, (state, action) => {
      state.todos = state.todos.map(todo =>
        todo.id.toString() === action.payload.id.toString()
          ? { ...todo, ...action.payload }
          : todo
      );
    });
    builder.addCase(updateTodo.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
    builder.addCase(deleteTodo.fulfilled, (state, action) => {
      state.todos = state.todos.filter(
        todo => todo.id.toString() !== action.payload.id.toString()
      );
    });
    builder.addCase(deleteTodo.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
    builder.addCase(getProjects.fulfilled, (state, action) => {
      state.projects = action.payload;
    });
    builder.addCase(getProjects.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
    builder.addCase(addNewProject.fulfilled, (state, action) => {
      state.projects.push(action.payload);
    });
    builder.addCase(addNewProject.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
  }
});

export default TodosSlice.reducer;