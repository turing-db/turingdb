import React, { useState } from "react";
import { Card, CardBody, Col, Container, DropdownItem, DropdownMenu, DropdownToggle, Nav, NavItem, NavLink, Row, TabContent, TabPane, UncontrolledDropdown } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import classnames from "classnames";
import Countdown from "react-countdown";
import { Link } from "react-router-dom";

// Import Images
import Img1 from "../../../assets/images/nft/img-01.jpg";
import Img2 from "../../../assets/images/nft/img-02.jpg";
import Img3 from "../../../assets/images/nft/img-03.jpg";
import Img5 from "../../../assets/images/nft/img-05.jpg";
import Img6 from "../../../assets/images/nft/img-06.jpg";

import ImgGif3 from "../../../assets/images/nft/gif/img-3.gif";
import ImgGif4 from "../../../assets/images/nft/gif/img-4.gif";

import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import SImg10 from "../../../assets/images/small/img-10.jpg";
import SImg11 from "../../../assets/images/small/img-11.jpg";
import SImg12 from "../../../assets/images/small/img-12.jpg";

//SimpleBar
import SimpleBar from "simplebar-react";


const ItemDetails = () => {
    document.title = "Item Details | Velzon - React Admin & Dashboard Template";

    const [customActiveTab, setcustomActiveTab] = useState("1");
    const toggleCustom = tab => {
        if (customActiveTab !== tab) {
            setcustomActiveTab(tab);
        }
    };

    const renderer = ({ days, hours, minutes, seconds, completed }) => {
        if (completed) {
            // Render a completed state
            return <span>You are good to go!</span>;
        } else {
            return (
                <>
                    <h5 id="auction-time-1" className="mb-0">{days} : {hours} : {minutes} : {seconds}</h5>
                </>
            );
        }
    };

    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Item Details" pageTitle="NFT Marketplace" />
                    <Card>
                        <CardBody>
                            <Row className="g-4">
                                <Col lg={4}>
                                    <div className="sticky-side-div">
                                        <Card className="ribbon-box border shadow-none right">
                                            <div className="ribbon-two ribbon-two-danger"><span><i className="ri-fire-fill align-bottom"></i> Hot</span></div>
                                            <img src={Img5} alt="" className="img-fluid rounded" />
                                            <div className="position-absolute bottom-0 p-3">
                                                <div className="position-absolute top-0 end-0 start-0 bottom-0 bg-white opacity-25"></div>
                                                <Row className="justify-content-center">
                                                    <Col className="col-3">
                                                        <img src={Img2} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                    <Col className="col-3">
                                                        <img src={Img3} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                    <Col className="col-3">
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-3.gif"} alt="" className="img-fluid rounded h-100 object-cover" />
                                                    </Col>
                                                    <Col className="col-3">
                                                        <img src={Img6} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                </Row>
                                            </div>
                                        </Card>
                                        <div className="hstack gap-2">
                                            <button className="btn btn-success w-100">Place Bid</button>
                                        </div>
                                    </div>
                                </Col>
                                <Col lg={8}>
                                    <div>
                                        <UncontrolledDropdown className="float-end">
                                            <DropdownToggle tag="a" className="btn btn-ghost-primary btn-icon" role="button">
                                                <i className="ri-more-fill align-middle fs-16"></i>
                                            </DropdownToggle>
                                            <DropdownMenu className="dropdown-menu-end">
                                                <DropdownItem href="#" className="view-item-btn"><i className="ri-eye-fill align-bottom me-2 text-muted"></i>View</DropdownItem>
                                                <DropdownItem href="#" className="edit-item-btn"><i className="ri-pencil-fill align-bottom me-2 text-muted"></i> Edit</DropdownItem>
                                                <DropdownItem href="#" className="remove-item-btn"><i className="ri-delete-bin-fill align-bottom me-2 text-muted"></i> Delete</DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>

                                        <span className="badge badge-soft-info mb-3 fs-12"><i className="ri-eye-line me-1 align-bottom"></i> 8,634 people views this</span>
                                        <h4>Patterns Arts & Culture</h4>
                                        <div className="hstack gap-3 flex-wrap">
                                            <div className="text-muted">Creators : <Link to="#" className="text-primary fw-medium">Nancy Martino</Link></div>
                                            <div className="vr"></div>
                                            <div className="text-muted">Seller : <span className="text-body fw-medium">Rickey Teran</span></div>
                                            <div className="vr"></div>
                                            <div className="text-muted">Published : <span className="text-body fw-medium">29 April, 2022</span></div>
                                        </div>
                                        <div className="d-flex flex-wrap gap-2 align-items-center mt-3">
                                            <div className="text-muted fs-16">
                                                <span className="mdi mdi-star text-warning"></span>
                                                <span className="mdi mdi-star text-warning"></span>
                                                <span className="mdi mdi-star text-warning"></span>
                                                <span className="mdi mdi-star text-warning"></span>
                                                <span className="mdi mdi-star text-warning"></span>
                                            </div>
                                            <div className="text-muted">( 5.50k Customer Review )</div>
                                        </div>
                                        <Row className="mt-4">
                                            <Col lg={3} sm={6}>
                                                <div className="p-2 border border-dashed rounded text-center">
                                                    <div>
                                                        <p className="text-muted fw-medium mb-1">Price :</p>
                                                        <h5 className="fs-17 text-success mb-0"><i className="mdi mdi-ethereum me-1"></i> 83.06 ETH</h5>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={3} sm={6}>
                                                <div className="p-2 border border-dashed rounded text-center">
                                                    <div>
                                                        <p className="text-muted fw-medium mb-1">Highest bid</p>
                                                        <h5 className="fs-17 mb-0">104.63 ETH</h5>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={3} sm={6}>
                                                <div className="p-2 border border-dashed rounded text-center">
                                                    <div>
                                                        <p className="text-muted fw-medium mb-1">Stock</p>
                                                        <h5 className="fs-17 mb-0">12/58 Sale</h5>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={3} sm={6}>
                                                <div className="p-2 border border-dashed rounded text-center">
                                                    <div>
                                                        <p className="text-muted fw-medium mb-1">Auction Ends:</p>
                                                        <Countdown date="2024/3/19" renderer={renderer} />
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                        <div className="mt-4 text-muted">
                                            <h5 className="fs-14">Description :</h5>
                                            <p>Cultural patterns are the similar behaviors within similar situations we witness due to shared beliefs, values, norms and social practices that are steady over time. In art, a pattern is a repetition of specific visual elements. The dictionary.com definition of "pattern" is: an arrangement of repeated or corresponding parts, decorative motifs, etc.</p>
                                        </div>
                                        <div className="product-content mt-5">
                                            <h5 className="fs-14 mb-3">Product Description :</h5>

                                            <Nav tabs className="nav-tabs-custom nav-success">
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            active: customActiveTab === "1",
                                                        })}
                                                        onClick={() => {
                                                            toggleCustom("1");
                                                        }}
                                                    >
                                                        Place Bids
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            active: customActiveTab === "2",
                                                        })}
                                                        onClick={() => {
                                                            toggleCustom("2");
                                                        }}
                                                    >
                                                        Additional Information
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            active: customActiveTab === "3",
                                                        })}
                                                        onClick={() => {
                                                            toggleCustom("3");
                                                        }}
                                                    >
                                                        Details
                                                    </NavLink>
                                                </NavItem>
                                            </Nav>
                                            <TabContent
                                                activeTab={customActiveTab}
                                                className="border border-top-0 p-4"
                                                id="nav-tabContent"
                                            >
                                                <TabPane
                                                    id="nav-speci"
                                                    tabId="1"
                                                >
                                                    <div className="table-responsive">
                                                        <table className="table align-middle table-nowrap mb-0">
                                                            <tbody>
                                                                <tr>
                                                                    <th scope="row">
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={Img1} alt="" className="avatar-xs rounded object-cover" />
                                                                            <Link to="/apps-nft-item-details" className="link-dark"><span className="mb-0 ms-2">Brave Tigers NFT</span></Link>
                                                                        </div>
                                                                    </th>
                                                                    <td>0.235 ETH</td>
                                                                    <td>
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={avatar1} alt="" className="avatar-xs rounded object-cover" />
                                                                            <div className="ms-2">
                                                                                <Link to="#!"><h6 className="mb-1">Alexis Clarke</h6></Link>
                                                                                <p className="text-muted mb-0">Creators</p>
                                                                            </div>
                                                                        </div>
                                                                    </td>
                                                                    <td>29 min ago</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={Img3} alt="" className="avatar-xs rounded object-cover" />
                                                                            <Link to="/apps-nft-item-details" className="link-dark"><span className="mb-0 ms-2">Creative filtered portrait</span></Link>
                                                                        </div>
                                                                    </th>
                                                                    <td>571.24 ETH</td>
                                                                    <td>
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={avatar3} alt="" className="avatar-xs rounded object-cover" />
                                                                            <div className="ms-2">
                                                                                <Link to="#!"><h6 className="mb-1">Glen Matney</h6></Link>
                                                                                <p className="text-muted mb-0">Creators</p>
                                                                            </div>
                                                                        </div>
                                                                    </td>
                                                                    <td>37 min ago</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={"https://img.themesbrand.com/velzon/images/img-4.gif"} alt="" className="avatar-xs rounded object-cover" />
                                                                            <Link to="/apps-nft-item-details" className="link-dark"><span className="mb-0 ms-2">Evolved Reality</span></Link>
                                                                        </div>
                                                                    </th>
                                                                    <td>130.39 ETH</td>
                                                                    <td>
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={avatar5} alt="" className="avatar-xs rounded object-cover" />
                                                                            <div className="ms-2">
                                                                                <Link to="#!"><h6 className="mb-1">Herbert Stokes</h6></Link>
                                                                                <p className="text-muted mb-0">Creators</p>
                                                                            </div>
                                                                        </div>
                                                                    </td>
                                                                    <td>1 hrs ago</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={Img6} alt="" className="avatar-xs rounded object-cover" />
                                                                            <Link to="/apps-nft-item-details" className="link-dark"><span className="mb-0 ms-2">Robotic Body Art</span></Link>
                                                                        </div>
                                                                    </th>
                                                                    <td>81.72 ETH</td>
                                                                    <td>
                                                                        <div className="d-flex align-items-center">
                                                                            <img src={avatar8} alt="" className="avatar-xs rounded object-cover" />
                                                                            <div className="ms-2">
                                                                                <Link to="#!"><h6 className="mb-1">Michael Morris</h6></Link>
                                                                                <p className="text-muted mb-0">Creators</p>
                                                                            </div>
                                                                        </div>
                                                                    </td>
                                                                    <td>1 hrs ago</td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </div>
                                                </TabPane>
                                                <TabPane
                                                    id="nav-detail"
                                                    tabId="2"
                                                >
                                                    <div className="table-responsive">
                                                        <table className="table mb-0">
                                                            <tbody>
                                                                <tr>
                                                                    <th scope="row" style={{ width: "200px" }}>Size</th>
                                                                    <td>650 x 650px (66.8 KB)</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">Brand</th>
                                                                    <td>Patterns arts & culture </td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">Formats</th>
                                                                    <td>JPEG / PNG / PDF</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">Token</th>
                                                                    <td>VLZ74516523</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">Blockchain</th>
                                                                    <td>Ethereum</td>
                                                                </tr>
                                                                <tr>
                                                                    <th scope="row">Contacts</th>
                                                                    <td>E545D145S5646544DS541SFDB213C5Z</td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </div>
                                                </TabPane>
                                                <TabPane
                                                    id="nav-detail"
                                                    tabId="3"
                                                >
                                                    <div>
                                                        <h5 className="font-size-16 mb-3">Patterns arts & culture</h5>
                                                        <p>Cultural patterns are the similar behaviors within similar situations we witness due to shared beliefs, values, norms and social practices that are steady over time. In art, a pattern is a repetition of specific visual elements. The dictionary.com definition of "pattern" is: an arrangement of repeated or corresponding parts, decorative motifs, etc.</p>
                                                        <div>
                                                            <p className="mb-2"><i className="mdi mdi-circle-medium me-1 text-muted align-middle"></i> On digital or printed media</p>
                                                            <p className="mb-2"><i className="mdi mdi-circle-medium me-1 text-muted align-middle"></i> For commercial and personal projects</p>
                                                            <p className="mb-2"><i className="mdi mdi-circle-medium me-1 text-muted align-middle"></i> From anywhere in the world</p>
                                                            <p className="mb-0"><i className="mdi mdi-circle-medium me-1 text-muted align-middle"></i> Full copyrights sale</p>
                                                        </div>
                                                    </div>
                                                </TabPane>
                                            </TabContent>
                                        </div>
                                        <div className="mt-5">
                                            <div>
                                                <h5 className="fs-14 mb-3">Ratings & Reviews</h5>
                                            </div>
                                            <Row className="gy-4 gx-0">
                                                <Col lg={4}>
                                                    <div>
                                                        <div className="pb-3">
                                                            <div className="bg-light px-3 py-2 rounded-2 mb-2">
                                                                <div className="d-flex align-items-center">
                                                                    <div className="flex-grow-1">
                                                                        <div className="fs-16 align-middle text-warning">
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-half-fill"></i>
                                                                        </div>
                                                                    </div>
                                                                    <div className="flex-shrink-0">
                                                                        <h6 className="mb-0">4.8 out of 5</h6>
                                                                    </div>
                                                                </div>
                                                            </div>
                                                            <div className="text-center">
                                                                <div className="text-muted">Total <span className="fw-medium">7.32k</span> reviews
                                                                </div>
                                                            </div>
                                                        </div>

                                                        <div className="mt-3">
                                                            <Row className="align-items-center g-2">
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0">5 star</h6>
                                                                    </div>
                                                                </Col>
                                                                <Col>
                                                                    <div className="p-2">
                                                                        <div className="progress animated-progress progress-sm">
                                                                            <div className="progress-bar bg-success" role="progressbar" style={{ width: "50.16%" }} aria-valuenow="50.16" aria-valuemin="0" aria-valuemax="100"></div>
                                                                        </div>
                                                                    </div>
                                                                </Col>
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0 text-muted">2758</h6>
                                                                    </div>
                                                                </Col>
                                                            </Row>

                                                            <Row className="align-items-center g-2">
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0">4 star</h6>
                                                                    </div>
                                                                </Col>
                                                                <Col>
                                                                    <div className="p-2">
                                                                        <div className="progress animated-progress progress-sm">
                                                                            <div className="progress-bar bg-success" role="progressbar" style={{ width: "19.32%" }} aria-valuenow="19.32" aria-valuemin="0" aria-valuemax="100"></div>
                                                                        </div>
                                                                    </div>
                                                                </Col>
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0 text-muted">1063</h6>
                                                                    </div>
                                                                </Col>
                                                            </Row>

                                                            <Row className="align-items-center g-2">
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0">3 star</h6>
                                                                    </div>
                                                                </Col>
                                                                <Col>
                                                                    <div className="p-2">
                                                                        <div className="progress animated-progress progress-sm">
                                                                            <div className="progress-bar bg-success" role="progressbar" style={{ width: "18.12%" }} aria-valuenow="18.12" aria-valuemin="0" aria-valuemax="100"></div>
                                                                        </div>
                                                                    </div>
                                                                </Col>
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0 text-muted">997</h6>
                                                                    </div>
                                                                </Col>
                                                            </Row>

                                                            <Row className="align-items-center g-2">
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0">2 star</h6>
                                                                    </div>
                                                                </Col>
                                                                <Col>
                                                                    <div className="p-2">
                                                                        <div className="progress animated-progress progress-sm">
                                                                            <div className="progress-bar bg-warning" role="progressbar" style={{ width: "7.42%" }} aria-valuenow="7.42" aria-valuemin="0" aria-valuemax="100"></div>
                                                                        </div>
                                                                    </div>
                                                                </Col>

                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0 text-muted">408</h6>
                                                                    </div>
                                                                </Col>
                                                            </Row>

                                                            <Row className="align-items-center g-2">
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0">1 star</h6>
                                                                    </div>
                                                                </Col>
                                                                <Col>
                                                                    <div className="p-2">
                                                                        <div className="progress animated-progress progress-sm">
                                                                            <div className="progress-bar bg-danger" role="progressbar" style={{ width: "4.98%" }} aria-valuenow="4.98" aria-valuemin="0" aria-valuemax="100"></div>
                                                                        </div>
                                                                    </div>
                                                                </Col>
                                                                <Col className="col-auto">
                                                                    <div className="p-2">
                                                                        <h6 className="mb-0 text-muted">274</h6>
                                                                    </div>
                                                                </Col>
                                                            </Row>

                                                        </div>
                                                    </div>
                                                </Col>

                                                <Col lg={8}>
                                                    <div className="ps-lg-4">
                                                        <div className="d-flex flex-wrap align-items-start gap-3">
                                                            <h5 className="fs-14">Reviews: </h5>
                                                        </div>

                                                        <SimpleBar className="me-lg-n3 pe-lg-4" style={{ maxHeight: "225px" }}>
                                                            <ul className="list-unstyled mb-0">
                                                                <li className="py-2">
                                                                    <div className="border border-dashed rounded p-3">
                                                                        <div className="d-flex align-items-start mb-3">
                                                                            <div className="hstack gap-3">
                                                                                <div className="text-warning">
                                                                                    <i className="mdi mdi-star"></i>
                                                                                    <i className="mdi mdi-star"></i>
                                                                                    <i className="mdi mdi-star"></i>
                                                                                    <i className="mdi mdi-star"></i>
                                                                                    <i className="mdi mdi-star"></i>
                                                                                </div>
                                                                                <div className="vr"></div>
                                                                                <div className="flex-grow-1">
                                                                                    <h6 className="mb-0"> Superb Artwork</h6>
                                                                                </div>
                                                                            </div>
                                                                        </div>

                                                                        <div className="d-flex flex-grow-1 gap-2 mb-3">
                                                                            <Link to="#" className="d-block">
                                                                                <img src={SImg12} alt="" className="avatar-sm rounded object-cover" />
                                                                            </Link>
                                                                            <Link to="#" className="d-block">
                                                                                <img src={SImg11} alt="" className="avatar-sm rounded object-cover" />
                                                                            </Link>
                                                                            <Link to="#" className="d-block">
                                                                                <img src={SImg10} alt="" className="avatar-sm rounded object-cover" />
                                                                            </Link>
                                                                        </div>

                                                                        <div className="d-flex align-items-end">
                                                                            <div className="flex-grow-1">
                                                                                <h5 className="fs-14 mb-0">Henry</h5>
                                                                            </div>

                                                                            <div className="flex-shrink-0">
                                                                                <p className="text-muted fs-13 mb-0">12 Jul, 21</p>
                                                                            </div>
                                                                        </div>
                                                                    </div>
                                                                </li>
                                                                <li className="py-2">
                                                                    <div className="border border-dashed rounded p-3">
                                                                        <div className="d-flex align-items-start mb-3">
                                                                            <div className="hstack gap-3">
                                                                                <div className="badge rounded-pill bg-success mb-0">
                                                                                    <i className="mdi mdi-star"></i> 4.0
                                                                                </div>
                                                                                <div className="vr"></div>
                                                                                <div className="flex-grow-1">
                                                                                    <p className="text-muted mb-0"> Great at this price, Product quality and look is awesome.</p>
                                                                                </div>
                                                                            </div>
                                                                        </div>
                                                                        <div className="d-flex align-items-end">
                                                                            <div className="flex-grow-1">
                                                                                <h5 className="fs-14 mb-0">Nancy</h5>
                                                                            </div>

                                                                            <div className="flex-shrink-0">
                                                                                <p className="text-muted fs-13 mb-0">06 Jul, 21</p>
                                                                            </div>
                                                                        </div>
                                                                    </div>
                                                                </li>
                                                                <li className="py-2">
                                                                    <div className="border border-dashed rounded p-3">
                                                                        <div className="d-flex align-items-start mb-3">
                                                                            <div className="hstack gap-3">
                                                                                <div className="badge rounded-pill bg-success mb-0">
                                                                                    <i className="mdi mdi-star"></i> 4.2
                                                                                </div>
                                                                                <div className="vr"></div>
                                                                                <div className="flex-grow-1">
                                                                                    <p className="text-muted mb-0">Good product. I am so happy.</p>
                                                                                </div>
                                                                            </div>
                                                                        </div>
                                                                        <div className="d-flex align-items-end">
                                                                            <div className="flex-grow-1">
                                                                                <h5 className="fs-14 mb-0">Joseph</h5>
                                                                            </div>

                                                                            <div className="flex-shrink-0">
                                                                                <p className="text-muted fs-13 mb-0">06 Jul, 21</p>
                                                                            </div>
                                                                        </div>
                                                                    </div>
                                                                </li>
                                                                <li className="py-2">
                                                                    <div className="border border-dashed rounded p-3">
                                                                        <div className="d-flex align-items-start mb-3">
                                                                            <div className="hstack gap-3">
                                                                                <div className="badge rounded-pill bg-success mb-0">
                                                                                    <i className="mdi mdi-star"></i> 4.1
                                                                                </div>
                                                                                <div className="vr"></div>
                                                                                <div className="flex-grow-1">
                                                                                    <p className="text-muted mb-0">Nice Product, Good Quality.</p>
                                                                                </div>
                                                                            </div>
                                                                        </div>
                                                                        <div className="d-flex align-items-end">
                                                                            <div className="flex-grow-1">
                                                                                <h5 className="fs-14 mb-0">Jimmy</h5>
                                                                            </div>

                                                                            <div className="flex-shrink-0">
                                                                                <p className="text-muted fs-13 mb-0">24 Jun, 21</p>
                                                                            </div>
                                                                        </div>
                                                                    </div>
                                                                </li>
                                                            </ul>
                                                        </SimpleBar>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                </Col>
                            </Row>
                        </CardBody>
                    </Card>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default ItemDetails;
