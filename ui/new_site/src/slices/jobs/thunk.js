import { createAsyncThunk } from "@reduxjs/toolkit";
//Include Both Helper File with needed methods
import {
    getJobApplicationList as getApplicationListApi
} from "../../helpers/fakebackend_helper";

export const getApplicationList = createAsyncThunk("jobs/getJobApplicationList", async () => {
    try {
        const response = getApplicationListApi();
        return response;
    } catch (error) {
        return error;
    }
});

