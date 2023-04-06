import { createAsyncThunk } from "@reduxjs/toolkit";
//Include Both Helper File with needed methods
import {
    getAPIKey as getAPIKeyApi
} from "../../helpers/fakebackend_helper";

export const getAPIKey = createAsyncThunk("apiKey/getAPIKey", async () => {
    try {
        const response = getAPIKeyApi();
        return response;
    } catch (error) {
        return error;
    }
});

