import React from "react";
import { Card, CardBody } from "reactstrap";

function UpcommingEvents(props) {
  
  const getTime = (params) => {
    params = new Date(params);
    if (params.getHours() != null) {
      const hour = params.getHours();
      const minute = params.getMinutes() ? params.getMinutes() : "00";
      return hour + ":" + minute;
    }
  };
  const tConvert = (time) => {
    const t = time.split(":");
    var hours = t[0];
    var minutes = t[1];
    var newformat = hours >= 12 ? "PM" : "AM";
    hours = hours % 12;
    hours = hours ? hours : 12;
    minutes = minutes < 10 ? "0" + minutes : minutes;
    return hours + ":" + minutes + " " + newformat;
  };

  const str_dt = function formatDate(date) {
    const monthNames = [
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December",
    ];
    var d = new Date(date),
      month = "" + monthNames[d.getMonth()],
      day = "" + d.getDate(),
      year = d.getFullYear();
    if (month.length < 2) month = "0" + month;
    if (day.length < 2) day = "0" + day;
    return [day + " " + month, year].join(",");
  };

  const category = props.event.className.split("-");

  var e_dt = props.event.end ? props.event.end : null;
  if (e_dt === "Invalid Date" || e_dt === undefined) {
    e_dt = null;
  } else {
    const newDate = new Date(e_dt).toLocaleDateString('en', { year: 'numeric', month: 'numeric', day: 'numeric' });
    e_dt = new Date(newDate)
      .toLocaleDateString("en-GB", {
        day: "numeric",
        month: "short",
        year: "numeric",
      })
      .split(" ")
      .join(" ");
  }

  const st_date = props.event.start ? str_dt(props.event.start) : null;
  const ed_date = props.event.end ? str_dt(props.event.end) : null;
  if (st_date === ed_date) {
    e_dt = null;
  }
  var startDate = props.event.start;
  if (startDate === "Invalid Date" || startDate === undefined) {
    startDate = null;
  } else {
    const newDate = new Date(startDate).toLocaleDateString('en', { year: 'numeric', month: 'numeric', day: 'numeric' });
    startDate = new Date(newDate)
      .toLocaleDateString("en-GB", {
        day: "numeric",
        month: "short",
        year: "numeric",
      })
      .split(" ")
      .join(" ");
  }

  var end_dt = e_dt ? " to " + e_dt : "";
  var e_time_s = tConvert(getTime(props.event.start));
  var e_time_e = tConvert(getTime(props.event.end));

  if (e_time_s === e_time_e) {
    e_time_s = "Full day event";
    e_time_e = null;
  }
  e_time_e = e_time_e ? " to " + e_time_e : "";

  return (
    <Card className="mb-3">
      <CardBody>
        <div className="d-flex mb-3">
          <div className="flex-grow-1">
            <i
              className={"mdi mdi-checkbox-blank-circle me-2 text-" + category[2]}
            ></i>
            <span className="fw-medium">
              {startDate} {end_dt}
            </span>
          </div>
          <div className="flex-shrink-0">
            <small className="badge badge-soft-primary ms-auto">
              {e_time_s} {e_time_e}
            </small>
          </div>
        </div>
        <h6 className="card-title fs-16">{props.event.title}</h6>
        <p className="text-muted text-truncate-two-lines mb-0">
          {props.event.description}
        </p>
      </CardBody>
    </Card>
  );
}

export default UpcommingEvents;