import React, { useState } from 'react';

import { Link } from 'react-router-dom';
import { Card, CardBody, CardHeader, Col, Row } from 'reactstrap';

//SimpleBar
import SimpleBar from "simplebar-react";

import avtar10 from "../../assets/images/users/avatar-10.jpg";
import avtar1 from "../../assets/images/users/avatar-1.jpg";
import avtar9 from "../../assets/images/users/avatar-9.jpg";
import avtar2 from "../../assets/images/users/avatar-2.jpg";
import avtar5 from "../../assets/images/users/avatar-5.jpg";
import avtar8 from "../../assets/images/users/avatar-8.jpg";

const Candidates = () => {
    const candidateListData =
        [
            { id: 1, img: avtar10, name: "Tonya Noble", username: "@tonya", designation: "Web Developer" },
            { id: 2, img: avtar1, name: "Nicholas Ball", username: "@nicholas", designation: "Assistant / Store Keeper" },
            { id: 3, img: avtar9, name: "Zynthia Marrow", username: "@zynthia", designation: "Full Stack Engineer" },
            { id: 4, img: avtar2, name: "Cheryl Moore", username: "@cheryl", designation: "Product Designer" },
            { id: 5, img: avtar5, name: "Jennifer Bailey", username: "@jennifer", designation: "Marketing Director" },
            { id: 6, img: avtar8, name: "Hadley Leonard", username: "@hadley", designation: "Executive, HR Operations" }
        ];

    const [info, setInfo] = useState([]);

    const [candidateList, setCandidateList] = useState(candidateListData);

    const searchCandidate = (ele) => {
        let search = ele.target.value;
        if (search) {
            search = search.toUpperCase();
            setCandidateList(candidateListData.filter((data) => (
                data.name.toUpperCase().includes(search) ||
                data.username.toUpperCase().includes(search)
            ))
            );
        } else {
            setCandidateList(candidateListData);
        }
    };

    const favouriteBtn = (ele) => {
        if (ele.closest("button").classList.contains("active")) {
            ele.closest("button").classList.remove("active");
        } else {
            ele.closest("button").classList.add("active");
        }
    };

    return (
        <React.Fragment>
            <Col className="col-xxl-4">
                <Card className="card">
                    <CardHeader className="card-header">
                        <div className="d-flex align-items-center">
                            <h6 className="card-title mb-0 flex-grow-1">
                                Popular Candidates
                            </h6>
                            <div className="flex-shrink-0">
                                <Link
                                    to="/apps-job-candidate-lists"
                                    className="link-primary"
                                >
                                    View All <i className="ri-arrow-right-line"></i>
                                </Link>
                            </div>
                        </div>
                    </CardHeader>
                    <Row className="row g-0">
                        <Col className="col-lg-6">
                            <CardBody className="card-body border-end">
                                <div className="search-box">
                                    <input
                                        type="text"
                                        className="form-control bg-light border-light"
                                        autoComplete="off"
                                        id="searchList"
                                        placeholder="Search candidate..."
                                        onKeyUp={(e) => searchCandidate(e)}
                                    />
                                    <i className="ri-search-line search-icon"></i>
                                </div>
                                <SimpleBar
                                    data-simplebar
                                    style={{ maxHeight: "190px" }}
                                    className="px-3 mx-n3"
                                >
                                    <ul className="list-unstyled mb-0 pt-2" id="candidate-list">
                                        {(candidateList || []).map((item, key) => (<li key={key}>
                                            <Link to="#" className="d-flex align-items-center py-2" onClick={() => setInfo(item)}>
                                                <div className="flex-shrink-0 me-2">
                                                    <div className="avatar-xs">
                                                        <img
                                                            src={item.img}
                                                            alt=""
                                                            className="img-fluid rounded-circle candidate-img"
                                                        />
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1">
                                                    <h5 className="fs-13 mb-1 text-truncate">
                                                        <span className="candidate-name">{item.name}</span>{" "}
                                                        <span className="text-muted fw-normal">{item.username}</span>
                                                    </h5>
                                                    <div className="d-none candidate-position">
                                                        {item.designation}
                                                    </div>
                                                </div>
                                            </Link>
                                        </li>))}
                                        {candidateList <= 0 && <div className="align-items-center ms-2 p-2">No matching records found</div>}
                                    </ul>
                                </SimpleBar>
                            </CardBody>
                        </Col>
                        <Col className="col-lg-6">
                            <CardBody className="card-body text-center">
                                <div className="avatar-md mb-3 mx-auto">
                                    <img
                                        src={info.img || avtar10}
                                        alt=""
                                        id="candidate-img"
                                        className="img-thumbnail rounded-circle shadow-none"
                                    />
                                </div>

                                <h5 id="candidate-name" className="mb-0">
                                    {info.name || "Tonya Noble"}
                                </h5>
                                <p id="candidate-position" className="text-muted">
                                    {info.designation || "Web Developer"}
                                </p>

                                <div className="d-flex gap-2 justify-content-center mb-3">
                                    <button
                                        type="button"
                                        className="btn avatar-xs p-0"
                                        data-bs-toggle="tooltip"
                                        data-bs-placement="top"
                                        title="Google"
                                    >
                                        <span className="avatar-title rounded-circle bg-light text-body">
                                            <i className="ri-google-line"></i>
                                        </span>
                                    </button>

                                    <button
                                        type="button"
                                        className="btn avatar-xs p-0"
                                        data-bs-toggle="tooltip"
                                        data-bs-placement="top"
                                        title="Linkedin"
                                    >
                                        <span className="avatar-title rounded-circle bg-light text-body">
                                            <i className="ri-linkedin-line"></i>
                                        </span>
                                    </button>
                                    <button
                                        type="button"
                                        className="btn avatar-xs p-0"
                                        data-bs-toggle="tooltip"
                                        data-bs-placement="top"
                                        title="Dribbble"
                                    >
                                        <span className="avatar-title rounded-circle bg-light text-body">
                                            <i className="ri-dribbble-fill"></i>
                                        </span>
                                    </button>
                                </div>

                                <div>
                                    <button
                                        type="button"
                                        className="btn btn-success custom-toggle w-100"
                                        data-bs-toggle="button"
                                        aria-pressed="false"
                                        onClick={(e) => favouriteBtn(e.target)}
                                    >
                                        <span className="icon-on">
                                            <i className="ri-add-line align-bottom me-1"></i> Follow
                                        </span>
                                        <span className="icon-off">
                                            <i className="ri-user-unfollow-line align-bottom me-1"></i>{" "}
                                            Unfollow
                                        </span>
                                    </button>
                                </div>
                            </CardBody>
                        </Col>
                    </Row>
                </Card>
                <Card className="card overflow-hidden shadow-none">
                    <CardBody className="card-body bg-soft-danger">
                        <div className="d-flex align-items-center">
                            <div className="flex-shrink-0">
                                <div className="avatar-sm">
                                    <div className="avatar-title bg-soft-danger text-danger rounded-circle fs-17">
                                        <i className="ri-gift-line"></i>
                                    </div>
                                </div>
                            </div>
                            <div className="flex-grow-1 ms-3">
                                <h6 className="fs-16">Invite your friends to Velzon</h6>
                                <p className="text-muted mb-0">
                                    Nor again is there anyone who loves or pursues or desires to
                                    obtain pain of itself, because it is pain, but because
                                    occasionally.
                                </p>
                            </div>
                        </div>
                        <div className="mt-3 text-end">
                            <Link to="#!" className="btn btn-danger">
                                Invite Friends
                            </Link>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default Candidates;