import { createAsyncThunk } from "@reduxjs/toolkit";
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

//Include Both Helper File with needed methods
import {
  getMailDetails as getMailDetailsApi,
  deleteMail as deleteMailApi
} from "../../helpers/fakebackend_helper";

export const getMailDetails = createAsyncThunk("mailbox/getMailDetails", async () => {
  try {
    const response = getMailDetailsApi();
    return response;
  } catch (error) {
    return error;
  }
});

export const deleteMail = createAsyncThunk("mailbox/deleteMail", async (mail) => {
  try {
    const response = deleteMailApi(mail);
    toast.success("Mail Delete Successfully", { autoClose: 3000 });
    return response;
  } catch (error) {
    toast.error("Mail Delete Failed", { autoClose: 3000 });
    return error;
  }
});