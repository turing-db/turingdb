import { createSlice } from "@reduxjs/toolkit";
import { getProducts, addNewProduct, updateProduct, deleteProducts, getOrders, addNewOrder, updateOrder, deleteOrder, getCustomers, addNewCustomer, updateCustomer, deleteCustomer, getSellers } from './thunk';
export const initialState = {
  products: [],
  orders: [],
  sellers: [],
  customers: [],
  error: {},
};

const EcommerceSlice = createSlice({
  name: 'EcommerceSlice',
  initialState,
  reducer: {},
  extraReducers: (builder) => {
    builder.addCase(getProducts.fulfilled, (state, action) => {
      state.products = action.payload.data;
    });

    builder.addCase(getProducts.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(addNewProduct.fulfilled, (state, action) => {
      state.products.push(action.payload);
    });

    builder.addCase(addNewProduct.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(updateProduct.fulfilled, (state, action) => {
      state.products = state.products.map(product =>
        product._id.toString() === action.payload.data._id.toString()
          ? { ...product, ...action.payload.data }
          : product
      );;
    });

    builder.addCase(updateProduct.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(deleteProducts.fulfilled, (state, action) => {
      state.products = (state.products || []).filter((product) => product._id.toString() !== action.payload.product.toString());
    });

    builder.addCase(deleteProducts.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getOrders.fulfilled, (state, action) => {
      state.orders = action.payload.data;
      state.isOrderCreated = false;
      state.isOrderSuccess = true;
    });

    builder.addCase(getOrders.rejected, (state, action) => {
      state.error = action.payload.error || null;
      state.isOrderCreated = false;
      state.isOrderSuccess = false;  
    });

    builder.addCase(addNewOrder.fulfilled, (state, action) => {
      state.orders.push(action.payload.data);
      state.isOrderCreated = true;
    });

    builder.addCase(addNewOrder.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(updateOrder.fulfilled, (state, action) => {
      state.orders = state.orders.map(order =>
        order._id.toString() === action.payload.data._id.toString()
          ? { ...order, ...action.payload.data }
          : order
      );
    });

    builder.addCase(updateOrder.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(deleteOrder.fulfilled, (state, action) => {
      state.orders = state.orders.filter(
        order => order._id.toString() !== action.payload.order.toString()
      );
    });

    builder.addCase(deleteOrder.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getSellers.fulfilled, (state, action) => {
      state.sellers = action.payload;
    });

    builder.addCase(getSellers.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(getCustomers.fulfilled, (state, action) => {
      state.customers = action.payload.data;
      state.isCustomerCreated = false;
      state.isCustomerSuccess = true;
    });

    builder.addCase(getCustomers.rejected, (state, action) => {
      state.error = action.payload.error || null;
      state.isCustomerCreated = false;
      state.isCustomerSuccess = false;
    });

    builder.addCase(addNewCustomer.fulfilled, (state, action) => {
      state.customers.push(action.payload.data);
      state.isCustomerCreated = true;
    });
    builder.addCase(addNewCustomer.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(updateCustomer.fulfilled, (state, action) => {
      state.customers = state.customers.map(customer =>
        customer._id.toString() === action.payload.data._id.toString()
          ? { ...customer, ...action.payload.data }
          : customer
      );
    });
    builder.addCase(updateCustomer.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });

    builder.addCase(deleteCustomer.fulfilled, (state, action) => {
      state.customers = state.customers.filter(
        customer => customer._id.toString() !== action.payload.customer.toString()
      );
    });
    builder.addCase(deleteCustomer.rejected, (state, action) => {
      state.error = action.payload.error || null;
    });
  }
});

export default EcommerceSlice.reducer;