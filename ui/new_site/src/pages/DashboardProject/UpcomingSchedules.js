import React from 'react';
import Flatpickr from "react-flatpickr";
import { Link } from 'react-router-dom';

const UpcomingSchedules = () => {
    return (
        <React.Fragment>
            <div className="col-xxl-4">
                <div className="card card-height-100">
                    <div className="card-header border-0">
                        <h4 className="card-title mb-0">Upcoming Schedules</h4>
                    </div>
                    <div className="card-body pt-0">
                        <div className="upcoming-scheduled">
                            <Flatpickr
                                className="form-control"
                                options={{
                                    dateFormat: "d M, Y",
                                    inline: true
                                }}
                            />
                        </div>

                        <h6 className="text-uppercase fw-semibold mt-4 mb-3 text-muted">Events:</h6>
                        <div className="mini-stats-wid d-flex align-items-center mt-3">
                            <div className="flex-shrink-0 avatar-sm">
                                <span className="mini-stat-icon avatar-title rounded-circle text-success bg-soft-success fs-4">
                                    09
                                </span>
                            </div>
                            <div className="flex-grow-1 ms-3">
                                <h6 className="mb-1">Development planning</h6>
                                <p className="text-muted mb-0">iTest Factory </p>
                            </div>
                            <div className="flex-shrink-0">
                                <p className="text-muted mb-0">9:20 <span className="text-uppercase">am</span></p>
                            </div>
                        </div>
                        <div className="mini-stats-wid d-flex align-items-center mt-3">
                            <div className="flex-shrink-0 avatar-sm">
                                <span className="mini-stat-icon avatar-title rounded-circle text-success bg-soft-success fs-4">
                                    12
                                </span>
                            </div>
                            <div className="flex-grow-1 ms-3">
                                <h6 className="mb-1">Design new UI and check sales</h6>
                                <p className="text-muted mb-0">Meta4Systems</p>
                            </div>
                            <div className="flex-shrink-0">
                                <p className="text-muted mb-0">11:30 <span className="text-uppercase">am</span></p>
                            </div>
                        </div>
                        <div className="mini-stats-wid d-flex align-items-center mt-3">
                            <div className="flex-shrink-0 avatar-sm">
                                <span className="mini-stat-icon avatar-title rounded-circle text-success bg-soft-success fs-4">
                                    25
                                </span>
                            </div>
                            <div className="flex-grow-1 ms-3">
                                <h6 className="mb-1">Weekly catch-up </h6>
                                <p className="text-muted mb-0">Nesta Technologies</p>
                            </div>
                            <div className="flex-shrink-0">
                                <p className="text-muted mb-0">02:00 <span className="text-uppercase">pm</span></p>
                            </div>
                        </div>
                        <div className="mini-stats-wid d-flex align-items-center mt-3">
                            <div className="flex-shrink-0 avatar-sm">
                                <span className="mini-stat-icon avatar-title rounded-circle text-success bg-soft-success fs-4">
                                    27
                                </span>
                            </div>
                            <div className="flex-grow-1 ms-3">
                                <h6 className="mb-1">James Bangs (Client) Meeting</h6>
                                <p className="text-muted mb-0">Nesta Technologies</p>
                            </div>
                            <div className="flex-shrink-0">
                                <p className="text-muted mb-0">03:45 <span className="text-uppercase">pm</span></p>
                            </div>
                        </div>

                        <div className="mt-3 text-center">
                            <Link to="#" className="text-muted text-decoration-underline">View all Events</Link>
                        </div>

                    </div>
                </div>
            </div>
        </React.Fragment>
    );
};

export default UpcomingSchedules;