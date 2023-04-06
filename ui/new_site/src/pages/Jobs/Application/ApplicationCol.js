import React from "react";
import { NavLink } from "react-router-dom";

const AppId = (cell) => {
  return (
    <NavLink to="#" className="text-body fw-bold">
      {cell.value ? cell.value : ""}
    </NavLink>
  );
};
const Name = (cell) => {
  return <React.Fragment>{cell.value}</React.Fragment>;
};
const Designation = (cell) => {
  return <React.Fragment>{cell.value}</React.Fragment>;
};

const Date = (cell) => {
  return <React.Fragment>{cell.value}</React.Fragment>;
};


const Contact = (cell) => {
  return <React.Fragment>{cell.value}</React.Fragment>;
};
const Type = (cell) => {
  return <React.Fragment>{cell.value}</React.Fragment>;
};

const Status = (cell) => {
  return (
    <React.Fragment>
      {cell.value === "New" ? (
        <span className="badge badge-soft-info text-uppercase">
          {cell.value}
        </span>
      ) : cell.value ==="Rejected" ? (
        <span className="badge badge-soft-danger text-uppercase">
          {cell.value}
        </span>
      ) : cell.value === "Pending" ? (
        <span className="badge badge-soft-warning text-uppercase">
          {cell.value}
        </span>
      ) : cell.value === "Approved" ? (
        <span className="badge badge-soft-success text-uppercase">
          {cell.value}
        </span>
      ) : null}
    </React.Fragment>
  );
};

export { AppId, Name, Designation, Date, Contact, Type, Status };
