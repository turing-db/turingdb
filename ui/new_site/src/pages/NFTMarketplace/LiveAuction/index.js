import React, { useState } from "react";
import { Card, CardBody, CardHeader, Col, Container, Row } from "reactstrap";
import Countdown from "react-countdown";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

import { aution, topCreator, topDrop } from "../../../common/data";

const LiveAuction = () => {
    document.title = "Live Auction | Velzon - React Admin & Dashboard Template";
    const [displayCategory, setCategory] = useState("All");

    const renderer = ({ days, hours, minutes, seconds, completed }) => {
        if (completed) {
            // Render a completed state
            return <span>You are good to go!</span>;
        } else {
            return (
                <>
                    <h5 id="auction-time-1" className="mb-0 text-white">{days} : {hours} : {minutes} : {seconds}</h5>
                </>
            );
        }
    };

    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Live Auction" pageTitle="NFT Marketplace" />
                    <Row>
                        <Col xxl={9}>
                            <Card>
                                <CardHeader className="border-0">
                                    <div className="d-lg-flex align-items-center">
                                        <div className="flex-grow-1">
                                            <h5 className="card-title mb-0">Live Auction</h5>
                                        </div>

                                        <div className="flex-shrink-0 mt-4 mt-lg-0">
                                            <ul className="nav nav-pills filter-btns" role="tablist">
                                                <li className="nav-item" role="presentation">
                                                    <button type="button" data-filter="all" onClick={() => setCategory("All")} className={displayCategory === "All" ? "nav-link fw-medium active" : "nav-link fw-medium"}>All Items</button>
                                                </li>
                                                <li className="nav-item" role="presentation">
                                                    <button type="button" data-filter="upto-15" onClick={() => setCategory("upto-15")} className={displayCategory === "upto-15" ? "nav-link fw-medium active" : "nav-link fw-medium"}>Up to 15%</button>
                                                </li>
                                                <li className="nav-item" role="presentation">
                                                    <button type="button" data-filter="upto-30" onClick={() => setCategory("upto-30")} className={displayCategory === "upto-30" ? "nav-link fw-medium active" : "nav-link fw-medium"}>Up to 30%</button>
                                                </li>
                                                <li className="nav-item" role="presentation">
                                                    <button type="button" data-filter="upto-40" onClick={() => setCategory("upto-40")} className={displayCategory === "upto-40" ? "nav-link fw-medium active" : "nav-link fw-medium"}>Up to 40%</button>
                                                </li>
                                            </ul>
                                        </div>

                                    </div>
                                </CardHeader>
                            </Card>
                            <Row>

                                {(aution.filter(({ category }) => displayCategory === category || displayCategory === "All")).map((item, key) => (<Col xxl={3} lg={4} md={6} className="product-item upto-15" key={key}>
                                    <Card className="explore-box card-animate">
                                        <div className="position-relative rounded overflow-hidden">
                                            <img src={item.img} alt="" className="card-img-top explore-img" />
                                            <div className="discount-time">
                                                <Countdown date={item.date} renderer={renderer} />
                                            </div>
                                        </div>
                                        <CardBody>
                                            <p className="fw-medium mb-0 float-end"><i className="mdi mdi-heart text-danger align-middle"></i> {item.likes}k </p>
                                            <h5 className="text-success"><i className="mdi mdi-ethereum"></i> {item.price} ETH </h5>
                                            <h6 className="fs-16 mb-3"><Link to="/apps-nft-item-details" className="link-dark">{item.title}</Link></h6>
                                            <div>
                                                <span className="text-muted float-end">Available: {item.available}</span>
                                                <span className="text-muted">Sold: {item.sold}</span>
                                                <div className="progress progress-sm mt-2">
                                                    <div className={"progress-bar progress-bar-striped bg-" + item.progressClass} role="progressbar" style={{ width: item.size }} aria-valuenow="67" aria-valuemin="0" aria-valuemax="100"></div>
                                                </div>
                                            </div>
                                        </CardBody>
                                    </Card>
                                </Col>))}
                            </Row>
                            <Row>
                                <Col lg={12}>
                                    <div className="text-center mb-3">
                                        <button className="btn btn-link text-success mt-2"><i className="mdi mdi-loading mdi-spin fs-20 align-middle me-2"></i> Load more </button>
                                    </div>
                                </Col>
                            </Row>
                        </Col>
                        <Col xxl={3}>
                            <Card>
                                <CardHeader className="d-flex align-items-center">
                                    <h6 className="card-title mb-0 flex-grow-1">Top Drop</h6>
                                    <Link className="text-muted" to="#">
                                        See All <i className="ri-arrow-right-line align-bottom"></i>
                                    </Link>
                                </CardHeader>
                                <CardBody>
                                    <div className="table-responsive table-card">
                                        <table className="table table-borderless align-middle">
                                            <tbody>
                                                {topDrop.map((item, key) => (<tr key={key}>
                                                    <td>
                                                        <div className="d-flex align-items-center">
                                                            <img src={item.img} alt="" className="avatar-sm object-cover rounded-circle" />
                                                            <div className="ms-2">
                                                                <Link to="/apps-nft-item-details"><h6 className="fs-15 mb-1">{item.title}</h6></Link>
                                                                <p className="mb-0 text-muted">Sold at {item.price} ETH</p>
                                                            </div>
                                                        </div>
                                                    </td>
                                                    <td><small>{item.time}</small></td>
                                                </tr>))}
                                            </tbody>
                                        </table>
                                    </div>
                                </CardBody>
                            </Card>
                            <Card>
                                <CardHeader className="d-flex align-items-center">
                                    <h6 className="card-title mb-0 flex-grow-1">Top Creator</h6>
                                    <Link className="text-muted" to="/apps-nft-item-details">
                                        See All <i className="ri-arrow-right-line align-bottom"></i>
                                    </Link>
                                </CardHeader>
                                <CardBody>
                                    <div className="table-responsive table-card">
                                        <table className="table table-borderless align-middle">
                                            <tbody>
                                                {topCreator.map((item, key) => (<tr key={key}>
                                                    <td>
                                                        <div className="d-flex align-items-center">
                                                            <img src={item.img} alt="" className="avatar-sm object-cover rounded-circle" />
                                                            <div className="ms-2">
                                                                <Link to="#!"><h6 className="fs-15 mb-1">{item.name}</h6></Link>
                                                                <p className="mb-0 text-muted">{item.products} Products</p>
                                                            </div>
                                                        </div>
                                                    </td>
                                                    <td><button className={item.isFollow ? "btn btn-sm btn-success" : "btn btn-sm btn-soft-success"}>{item.isFollow ? "Follow" : "Unfollow"}</button></td>
                                                </tr>))}
                                            </tbody>
                                        </table>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default LiveAuction;
