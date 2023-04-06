import React from 'react';
import { Container, Row, Col, Card, CardBody } from 'reactstrap';
import { Link } from 'react-router-dom';
import { discoverItemsData } from '../../../common/data';

const favouriteBtn = (ele) => {
    if (ele.closest("button").classList.contains("active")) {
        ele.closest("button").classList.remove("active");
    } else {
        ele.closest("button").classList.add("active");
    }
};

const DiscoverItems = () => {
    
    return (
        <React.Fragment>
            <section className="section">
                <Container>
                    <Row className="justify-content-center">
                        <Col lg={12}>
                            <div className="d-flex align-items-center mb-5">
                                <h2 className="mb-0 fw-semibold lh-base flex-grow-1">Discover Items</h2>
                                <Link to="/apps-nft-explore" className="btn btn-primary">View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                            </div>
                        </Col>
                    </Row>
                    <Row>
                        {discoverItemsData.map((item, key) => (
                            <Col key={key} lg={4}>
                                <Card className="explore-box card-animate border">
                                    <CardBody>
                                        <div className="d-flex align-items-center mb-3">
                                            <img src={item.cardImg} alt="" className="avatar-xs rounded-circle" />
                                            <div className="ms-2 flex-grow-1">
                                                <Link to="#!"><h6 className="mb-0 fs-15">{item.author}</h6></Link>
                                                <p className="mb-0 text-muted fs-13">{item.title}</p>
                                            </div>
                                            <div className="bookmark-icon">
                                                <button type="button" className={item.isIcon ? "btn btn-icon" : "btn btn-icon active"} data-bs-toggle="button" aria-pressed="true" onClick={(e) => favouriteBtn(e.target)}>
                                                    <i className="mdi mdi-cards-heart fs-16"></i></button>
                                            </div>
                                        </div>
                                        <div className="explore-place-bid-img overflow-hidden rounded">
                                            <img src={item.img} alt="" className="explore-img w-100" />
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
                </Container>
            </section>
        </React.Fragment>
    );
};

export default DiscoverItems;