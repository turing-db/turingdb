import { createAsyncThunk } from "@reduxjs/toolkit";
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

//Include Both Helper File with needed methods
import {
    getTicketsList as getTicketsListApi,
    addNewTicket as addNewTicketApi,
    updateTicket as updateTicketApi,
    deleteTicket as deleteTicketApi
} from "../../helpers/fakebackend_helper";

export const getTicketsList = createAsyncThunk("tickets/getTicketsList", async () => {
    try {
        const response = getTicketsListApi();
        return response;
    } catch (error) {
        return error;
    }
});

export const addNewTicket = createAsyncThunk("tickets/addNewTicket", async (ticket) => {
    try {
        const response = addNewTicketApi(ticket);
        const data = await response;
        toast.success("Ticket Added Successfully", { autoClose: 3000 });
        return data;
    } catch (error) {
        return error;
    }
});

export const updateTicket = createAsyncThunk("tickets/updateTicket", async (ticket) => {
    try {
        const response = updateTicketApi(ticket);
        const data = await response;
        toast.success("Ticket Updated Successfully", { autoClose: 3000 });
        return data;
    } catch (error) {
        toast.error("Ticket Updated Failed", { autoClose: 3000 });
        return error;
    }
});

export const deleteTicket = createAsyncThunk("tickets/deleteTicket", async (ticket) => {
    try {
        const response = deleteTicketApi(ticket);
        toast.success("Ticket Delete Successfully", { autoClose: 3000 });
        return { ticket, ...response };
    } catch (error) {
        toast.error("Ticket Delete Failed", { autoClose: 3000 });
        return error;
    }
});