import { createAsyncThunk } from "@reduxjs/toolkit";
//Include Both Helper File with needed methods
import {
    getTransationList as getTransationListApi,
    getOrderList as getOrderListApi
} from "../../helpers/fakebackend_helper";

export const getTransationList = createAsyncThunk("crypto/getTransationList", async () => {
    try {
        const response = getTransationListApi();
        return response;
    } catch (error) {
        return error;
    }
});

export const getOrderList = createAsyncThunk("crypto/getOrderList", async () => {
    try {
        const response = getOrderListApi();
        return response;
    } catch (error) {
        return error;
    }
});

