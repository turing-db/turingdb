import React, { useState } from "react";
import {
    Card,
    CardBody,
    CardHeader,
    Col,
    Container,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Row,
    UncontrolledCollapse,
    UncontrolledDropdown,
} from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

// RangeSlider

import Nouislider from "nouislider-react";
import "nouislider/distribute/nouislider.css";

import { expolreNow } from "../../../common/data/index";

const ExploreNow = () => {
    document.title = "Explore Now | Velzon - React Admin & Dashboard Template";
    const [NFTList, setNFTList] = useState(expolreNow);

    const favouriteBtn = (ele) => {
        if (ele.closest("button").classList.contains("active")) {
            ele.closest("button").classList.remove("active");
        } else {
            ele.closest("button").classList.add("active");
        }
    };

    const onUpdate = (value) => {
        setNFTList(
            expolreNow.filter(
                (NFT) => NFT.price >= value[0] && NFT.price <= value[1],
            )
        );
    };

    const category = (e) => {
        setNFTList(
            expolreNow.filter((item) => item.category === e));
    };

    const fileType = (e) => {
        setNFTList(
            expolreNow.filter((item) => item.fileType === e));
    };

    const salesType = (e) => {
        setNFTList(
            expolreNow.filter((item) => item.sales === e));
    };

    const searchNFT = () => {
        var searchProductList = document.getElementById("searchProductList");
        var inputVal = searchProductList.value.toLowerCase();
        function filterItems(arr, query) {
            return arr.filter(function (el) {
                return el.title.toLowerCase().indexOf(query.toLowerCase()) !== -1;
            });
        }
        var filterData = filterItems(expolreNow, inputVal);
        if (filterData.length === 0) {
            document.getElementById("noresult").style.display = "block";
            document.getElementById("loadmore").style.display = "none";
        } else {
            document.getElementById("noresult").style.display = "none";
            document.getElementById("loadmore").style.display = "block";
        }
        setNFTList(filterData);
    };

    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Explore Now" pageTitle="NFT Marketplace" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader className="border-0">
                                    <div className="d-flex align-items-center">
                                        <h5 className="card-title mb-0 flex-grow-1">
                                            Explore Product
                                        </h5>
                                        <div>
                                            <Link
                                                className="btn btn-success"
                                                id="filter-collapse"
                                                data-bs-toggle="collapse"
                                                to="#collapseExample"
                                            >
                                                <i className="ri-filter-2-line align-bottom"></i>
                                                Filters
                                            </Link>
                                        </div>
                                    </div>

                                    <UncontrolledCollapse toggler="#filter-collapse" defaultOpen>
                                        <Row className="row-cols-xxl-5 row-cols-lg-3 row-cols-md-2 row-cols-1 mt-3 g-3">
                                            <Col>
                                                <h6 className="text-uppercase fs-12 mb-2">Search</h6>

                                                <input
                                                    type="text"
                                                    className="form-control"
                                                    placeholder="Search product name"
                                                    autoComplete="off"
                                                    id="searchProductList"
                                                    onKeyUp={searchNFT}
                                                />
                                            </Col>
                                            <Col>
                                                <h6 className="text-uppercase fs-12 mb-2">
                                                    Select Category
                                                </h6>
                                                <select
                                                    className="form-control"
                                                    data-choices
                                                    name="select-category"
                                                    data-choices-search-false
                                                    id="select-category"
                                                    onChange={(e) => category(e.target.value)}
                                                >
                                                    <option value="">Select Category</option>
                                                    <option value="Artwork">Artwork</option>
                                                    <option value="3d Style">3d Style</option>
                                                    <option value="Photography">Photography</option>
                                                    <option value="Collectibles">Collectibles</option>
                                                    <option value="Crypto Card">Crypto Card</option>
                                                    <option value="Games">Games</option>
                                                    <option value="Music">Music</option>
                                                </select>
                                            </Col>
                                            <Col>
                                                <h6 className="text-uppercase fs-12 mb-2">File Type</h6>
                                                <select
                                                    className="form-control"
                                                    data-choices
                                                    name="file-type"
                                                    data-choices-search-false
                                                    id="file-type"
                                                    onChange={(e) => fileType(e.target.value)}
                                                >
                                                    <option value="">File Type</option>
                                                    <option value="jpg">Images</option>
                                                    <option value="mp4">Video</option>
                                                    <option value="mp3">Audio</option>
                                                    <option value="gif">Gif</option>
                                                </select>
                                            </Col>
                                            <Col>
                                                <h6 className="text-uppercase fs-12 mb-2">
                                                    Sales Type
                                                </h6>
                                                <select
                                                    className="form-control"
                                                    data-choices
                                                    name="all-sales-type"
                                                    data-choices-search-false
                                                    id="all-sales-type"
                                                    onChange={(e) => salesType(e.target.value)}
                                                >
                                                    <option value="">All Sales Type</option>
                                                    <option value="On Auction">On Auction</option>
                                                    <option value="Has Offers">Has Offers</option>
                                                </select>
                                            </Col>
                                            <Col>
                                                <h6 className="text-uppercase fs-12 mb-4">Price</h6>

                                                <Nouislider
                                                    range={{ min: 0, max: 2000 }}
                                                    tooltips={true}
                                                    start={[0, 1000]}
                                                    connect
                                                    className="slider"
                                                    id="range-product-price"
                                                    onSlide={onUpdate}
                                                />
                                            </Col>
                                        </Row>
                                    </UncontrolledCollapse>
                                </CardHeader>
                            </Card>
                        </Col>
                        <Col lg={12}>
                            <div className="d-flex align-items-center mb-4">
                                <div className="flex-grow-1">
                                    <p className="text-muted fs-14 mb-0">Result: 8745</p>
                                </div>
                                <div className="flex-shrink-0">
                                    <UncontrolledDropdown>
                                        <DropdownToggle
                                            tag="a"
                                            className="text-muted fs-14"
                                            role="button"
                                        >
                                            All View <i className="mdi mdi-chevron-down"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <DropdownItem to="#">Action</DropdownItem>
                                            <DropdownItem to="#">Another action</DropdownItem>
                                            <DropdownItem to="#">Something else here</DropdownItem>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                            </div>
                        </Col>
                    </Row>
                    <Row
                        className="row-cols-xxl-5 row-cols-xl-4 row-cols-lg-3 row-cols-md-2 row-cols-1"
                        id="explorecard-list"
                    >

                        {NFTList.map((item, key) => (<Col className="list-element" key={key}>
                            <Card className="explore-box card-animate">
                                <div className="explore-place-bid-img">
                                    <input type="hidden" className="form-control" id="1" />
                                    <div className="d-none">undefined</div>
                                    <img
                                        src={item.img}
                                        alt=""
                                        className="card-img-top explore-img"
                                    />
                                    <div className="bg-overlay"></div>
                                    <div className="place-bid-btn">

                                        <Link to="#!" className="btn btn-success">
                                            <i className="ri-auction-fill align-bottom me-1"></i> Place
                                            Bid
                                        </Link>
                                    </div>
                                </div>
                                <div className="bookmark-icon position-absolute top-0 end-0 p-2">

                                    <button
                                        type="button"
                                        className={item.isActive ? "btn btn-icon active" : "btn btn-icon"}
                                        data-bs-toggle="button"
                                        aria-pressed="true"
                                        onClick={(e) => favouriteBtn(e.target)}
                                    >
                                        <i className="mdi mdi-cards-heart fs-16"></i>
                                    </button>
                                </div>
                                <CardBody>

                                    <p className="fw-medium mb-0 float-end">
                                        <i className="mdi mdi-heart text-danger align-middle"></i>{" "}
                                        {item.likes}k
                                    </p>
                                    <h5 className="mb-1">
                                        <Link to="/apps-nft-item-details">{item.title}</Link>
                                    </h5>
                                    <p className="text-muted mb-0">{item.category}</p>
                                </CardBody>
                                <div className="card-footer border-top border-top-dashed">
                                    <div className="d-flex align-items-center">
                                        <div className="flex-grow-1 fs-14">

                                            <i className="ri-price-tag-3-fill text-warning align-bottom me-1"></i>
                                            Highest: <span className="fw-medium">{item.highestBid}ETH</span>
                                        </div>
                                        <h5 className="flex-shrink-0 fs-14 text-primary mb-0">
                                            {item.price}ETH
                                        </h5>
                                    </div>
                                </div>
                            </Card>
                        </Col>))}
                    </Row>
                    <div
                        className="py-4 text-center"
                        id="noresult"
                        style={{ display: "none" }}
                    >
                        <lord-icon
                            src="https://cdn.lordicon.com/msoeawqm.json"
                            trigger="loop"
                            colors="primary:#405189,secondary:#0ab39c"
                            style={{ width: "72px", height: "72px" }}
                        ></lord-icon>
                        <h5 className="mt-4">Sorry! No Result Found</h5>
                    </div>
                    <div className="text-center mb-3">
                        <button className="btn btn-link text-success mt-2" id="loadmore">
                            <i className="mdi mdi-loading mdi-spin fs-20 align-middle me-2"></i>
                            Load More
                        </button>
                    </div>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default ExploreNow;
