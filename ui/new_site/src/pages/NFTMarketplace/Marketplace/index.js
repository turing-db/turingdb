import React from "react";
import { Card, CardBody, Col, Container, Row } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

import { nftArtworkData, tradingArtworkData, popularCreatorsNFT, marketPlacewidget } from "../../../common/data";

import BgPattern from "../../../assets/images/nft/bg-pattern.png";

const Marketplace = () => {
    document.title = "Marketplace | Velzon - React Admin & Dashboard Template";

    const favouriteBtn = (ele) => {
        if (ele.closest("button").classList.contains("active")) {
            ele.closest("button").classList.remove("active");
        } else {
            ele.closest("button").classList.add("active");
        }
    };

    return (
        <React.Fragment>Fmar
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Marketplace" pageTitle="NFT Marketplace" />
                    <Row>
                        <Col xl={8}>
                            <Row>
                                {marketPlacewidget.map((item, key) => (<Col lg={4} md={6} key={key}>
                                    <Card>
                                        <CardBody>
                                            <Row className="g-1 mb-3">
                                                {item.subItem.map((item, key) => (<Col lg={6} key={key}>
                                                    {item.isChildren.map((item, key) => (<img src={item.img} alt="" className={"img-fluid rounded " + item.imgClass} key={key} />))}
                                                </Col>))}
                                            </Row>
                                            <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                            <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">{item.category} <span className="badge badge-soft-success">{item.quantity}</span></Link></h5>
                                        </CardBody>
                                    </Card>
                                </Col>))}
                            </Row>
                        </Col>

                        <Col xl={4}>
                            <div className="alert alert-danger">
                                Up to <b>50% OFF</b>, Hurry up before the stock ends
                            </div>
                            <Card className="bg-primary" style={{ backgroundImage: `url(${BgPattern})` }}>
                                <CardBody className="p-4">
                                    <span className="badge bg-info fw-bold mb-3 fs-12">Trending Artwork</span>
                                    <h2 className="text-white lh-base">Discover, Collect, Sell and Create your own NFT</h2>
                                    <p className="text-white-75 mb-3">Take advantage of the first text-based NFT. Select a text or write your own and let it live forever on the Ethereum blockchain you or future owners can add text to an existing ChainText token..</p>
                                    <div className="hstack gap-2">
                                        <Link to="/apps-nft-create" className="btn btn-success">Create NFT</Link>
                                        <Link to="/apps-nft-explore" className="btn btn-danger">Explore Now</Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card className="overflow-hidden shadow-none">
                                <CardBody className="bg-soft-danger text-danger fw-semibold d-flex">
                                    <div className="marquee">
                                        <div>
                                            <span>NFT art is a digital asset that is collectable, unique, and non-transferrable, Cortes explained. Every NFT is unique in it's creative design and cannot be duplicated, making them limited and rare. NFTs get their value because the transaction proves ownership of the art.</span>
                                            <span>NFT art is a digital asset that is collectable, unique, and non-transferrable, Cortes explained. Every NFT is unique in it's creative design and cannot be duplicated, making them limited and rare. NFTs get their value because the transaction proves ownership of the art.</span>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <div className="d-lg-flex align-items-center mb-4">
                                <div className="flex-grow-1">
                                    <h5 className="card-title mb-0 fw-bold fs-17">Trending Artwork</h5>
                                </div>
                                <div className="flex-shrink-0 mt-4 mt-lg-0">
                                    <Link to="/apps-nft-explore" className="btn btn-soft-primary">View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                </div>
                            </div>
                        </Col>
                    </Row>

                    <Row className="row-cols-xl-5 row-cols-lg-3 row-cols-md-2 row-cols-1">
                        {tradingArtworkData.map((item, key) => (
                            <Col key={key}>
                                <Card className="explore-box">
                                    <CardBody>
                                        <div className="d-flex align-items-center mb-3">
                                            <img src={item.img} alt="" className="avatar-xs rounded-circle" />
                                            <div className="ms-2 flex-grow-1">
                                                <h6 className="mb-0 fs-15">{item.author}</h6>
                                                <p className="mb-0 text-muted">{item.title}</p>
                                            </div>
                                            <div className="bookmark-icon">
                                                <button type="button"
                                                    className={item.isActive ? "btn btn-icon active " : "btn btn-icon"}
                                                    data-bs-toggle="button"
                                                    aria-pressed="true"
                                                    onClick={(e) => favouriteBtn(e.target)}
                                                >
                                                    <i className="mdi mdi-cards-heart fs-16"></i>
                                                </button>
                                            </div>
                                        </div>
                                        <div className="explore-place-bid-img overflow-hidden rounded">
                                            <img src={item.cardImg} alt="" className="img-fluid explore-img" />
                                            <div className="bg-overlay"></div>
                                            <div className="place-bid-btn">
                                                <Link to="#!" className="btn btn-success"><i className="ri-auction-fill align-bottom me-1"></i> Place Bid</Link>
                                            </div>
                                        </div>
                                        <div className="mt-3">
                                            <p className="fw-medium mb-0 float-end"><i className="mdi mdi-heart text-danger align-middle"></i> {item.likes} </p>
                                            <h5 className="text-success"><i className="mdi mdi-ethereum"></i> {item.price} </h5>
                                            <h6 className="fs-16 mb-0"><Link to="/apps-nft-item-details" className="link-dark">{item.category}</Link></h6>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        ))}
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <div className="d-lg-flex align-items-center mb-4">
                                <div className="flex-grow-1">
                                    <h5 className="card-title mb-0 fw-bold fs-17">Recent NFTs Artwork</h5>
                                </div>
                                <div className="flex-shrink-0 mt-4 mt-lg-0">
                                    <Link to="/apps-nft-creators" className="btn btn-soft-primary">View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                </div>
                            </div>
                        </Col>
                    </Row>

                    <Row className="row-cols-xl-5 row-cols-lg-3 row-cols-md-2 row-cols-1">
                        {nftArtworkData.map((item, key) => (
                            <Col key={key}>
                                <Card className="explore-box card-animate">
                                    <div className="bookmark-icon position-absolute top-0 end-0 p-2">
                                        <button type="button" className="btn btn-icon active" data-bs-toggle="button" aria-pressed="true"><i className="mdi mdi-cards-heart fs-16"></i></button>
                                    </div>
                                    <div className="explore-place-bid-img">
                                        <img src={item.img} alt="" className="card-img-top explore-img" />
                                        <div className="bg-overlay"></div>
                                        <div className="place-bid-btn">
                                            <Link to="#!" className="btn btn-success"><i className="ri-auction-fill align-bottom me-1"></i> Place Bid</Link>
                                        </div>
                                    </div>
                                    <CardBody>
                                        <p className="fw-medium mb-0 float-end"><i className="mdi mdi-heart text-danger align-middle"></i> {item.likes} </p>
                                        <h5 className="mb-1"><Link to="/apps-nft-item-details" className="link-dark">{item.title}</Link></h5>
                                        <p className="text-muted mb-0">{item.category}</p>
                                    </CardBody>
                                    <div className="card-footer border-top border-top-dashed">
                                        <div className="d-flex align-items-center">
                                            <div className="flex-grow-1 fs-14">
                                                <i className="ri-price-tag-3-fill text-warning align-bottom me-1"></i> Highest: <span className="fw-medium">{item.highest}</span>
                                            </div>
                                            <h5 className="flex-shrink-0 fs-14 text-primary mb-0">{item.price}</h5>
                                        </div>
                                    </div>
                                </Card>
                            </Col>
                        ))}
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <div className="d-lg-flex align-items-center mb-4">
                                <div className="flex-grow-1">
                                    <h5 className="card-title mb-0 fw-bold fs-17">Popular Creators</h5>
                                </div>
                                <div className="flex-shrink-0 mt-4 mt-lg-0">
                                    <Link to="/apps-nft-creators" className="btn btn-soft-primary">View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                </div>
                            </div>
                        </Col>
                    </Row>

                    <Row className="row-cols-xl-5 row-cols-lg-3 row-cols-md-2 row-cols-1">
                        {popularCreatorsNFT.map((item, key) => (
                            <Col key={key}>
                                <Card>
                                    <img src={item.cardImg} alt="" className="card-img-top object-cover" height="120" />
                                    <CardBody className="text-center">
                                        <img src={item.img} alt="" className="avatar-md mt-n5 rounded-circle mx-auto d-block object-cover" />
                                        <h5 className="mt-3 mb-1"><Link to="#!" className="link-dark">{item.author}</Link></h5>
                                        <p className="text-muted">{item.products} Products</p>
                                        <div>
                                            <button className={item.isFollow ? "btn btn-primary btn-sm" : "btn btn-soft-primary btn-sm"}>{item.isFollow ? "Follow" : "Unfollow"}</button>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        ))}
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default Marketplace;
