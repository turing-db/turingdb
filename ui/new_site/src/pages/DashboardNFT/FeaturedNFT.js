import React from 'react';
import { Card, CardBody, Col, Row } from 'reactstrap';
import { topartWork } from "../../common/data/index";
import { featuredNFTData } from "../../common/data/dashboardNFT";
// Swiper
import { Swiper, SwiperSlide } from "swiper/react";
import "swiper/css";
import "swiper/css/pagination";
import "swiper/css/navigation";
import { Navigation, Autoplay } from "swiper";
// Import Chart
import { TopArtworkChart } from "./DashboardNFTCharts";
//SimpleBar
import SimpleBar from "simplebar-react";
import { Link } from 'react-router-dom';
const FeaturedNFT = () => {
    return (
        <React.Fragment>
            <Row>
                <Col xxl={8}>
                    <div className="d-flex pt-2 mb-n4">
                        <h5 className="card-title fs-18 mb-0">Featured NFTs Artworks</h5>
                    </div>
                    <Swiper modules={[Navigation, Autoplay]}
                        slidesPerView={1}
                        spaceBetween={10}
                        navigation={{
                            nextEl: ".swiper-button-next",
                            prevEl: ".swiper-button-prev",
                        }}
                        breakpoints={{
                            640: {
                                slidesPerView: 2,
                                spaceBetween: 20,
                            },
                            768: {
                                slidesPerView: 2,
                                spaceBetween: 24,
                            },
                            1024: {
                                slidesPerView: 3,
                                spaceBetween: 24,
                            },
                        }}
                        loop={true}
                        autoplay={{ delay: 2500, disableOnInteraction: false }}
                        className="mySwiper marketplace-swiper rounded gallery-light pt-5">
                        <div className="swiper-wrapper">
                            {featuredNFTData.map((item, key) => (
                                <SwiperSlide key={key}>
                                    <div className="card explore-box card-animate rounded">
                                        <div className="bookmark-icon position-absolute top-0 end-0 p-2">
                                            <button type="button" className="btn btn-icon active" data-bs-toggle="button" aria-pressed="true"><i className="mdi mdi-cards-heart fs-16"></i></button>
                                        </div>
                                        <div className="explore-place-bid-img">
                                            <img src={item.img} alt="" className="img-fluid card-img-top explore-img" />
                                            <div className="bg-overlay"></div>
                                            <div className="place-bid-btn">
                                                <Link to="#" className="btn btn-success"><i className="ri-auction-fill align-bottom me-1"></i> Place Bid</Link>
                                            </div>
                                        </div>
                                        <CardBody>
                                            <p className="fw-medium mb-0 float-end"><i className="mdi mdi-heart text-danger align-middle"></i> {item.price} </p>
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
                                    </div>
                                </SwiperSlide>
                            ))}
                        </div>
                        <div className="swiper-button-next"></div>
                        <div className="swiper-button-prev"></div>
                    </Swiper>
                </Col>
                <Col xxl={4}>
                    <Card>
                        <div className="card-header align-items-center d-flex">
                            <h4 className="card-title mb-0 flex-grow-1">Top Artworks</h4>
                            <div className="flex-shrink-0">
                                <div>
                                    <button type="button" className="btn btn-soft-primary btn-sm">
                                        See All
                                    </button>
                                </div>
                            </div>
                        </div>
                        <CardBody>
                            <div className="table-responsive table-card">
                                <SimpleBar style={{ maxHeight: "405px" }}>
                                    <table className="table table-borderless align-middle">
                                        <tbody>
                                            {topartWork.map((item, key) => (
                                                <tr key={key}>
                                                    <td>
                                                        <div className="d-flex align-items-center">
                                                            <img src={item.img} alt="" className="avatar-sm rounded-circle" />
                                                            <div className="ms-3">
                                                                <Link to="#!">
                                                                    <h6 className="fs-15 mb-1">{item.title}</h6>
                                                                </Link>
                                                                <p className="mb-0 text-muted">{item.sales} Sales</p>
                                                            </div>
                                                        </div>
                                                    </td>
                                                    <td>
                                                        <TopArtworkChart seriesData={item.series} chartsColor={item.chartsColor} />
                                                    </td>
                                                    <td className="text-end">
                                                        <Link to="#!">
                                                            <h6 className="fs-15 mb-1">${item.collection}+</h6>
                                                        </Link>
                                                        <p className="mb-0 text-muted">Total USD</p>
                                                    </td>
                                                </tr>))}
                                        </tbody>
                                    </table>
                                </SimpleBar>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            </Row>
        </React.Fragment>
    );
};
export default FeaturedNFT;