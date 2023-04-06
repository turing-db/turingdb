import React from 'react';
import { Swiper, SwiperSlide } from "swiper/react";
import "swiper/css";
import "swiper/css/pagination";
import { Autoplay, Mousewheel } from "swiper";
import { watchlist } from "../../../common/data";
import { Card, CardBody, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from 'reactstrap';
import WatchListChart from './watchListChart';
const Watchlist = () => {
    return (
        <React.Fragment>
            <div className="d-flex align-items-center mb-3">
                <div className="flex-grow-1">
                    <h5 className="mb-0">Watchlist</h5>
                </div>
                <div className="flexshrink-0">
                    <button className="btn btn-success btn-sm"><i className="ri-star-line align-bottom"></i> Add Watchlist</button>
                </div>
            </div>

            <Swiper
                slidesPerView={4}
                spaceBetween={24}
                mousewheel={true}
                autoplay={{
                    delay: 2000,
                    disableOnInteraction: false,
                }}
                modules={[Autoplay, Mousewheel]}
                className="cryptoSlider">
                {(watchlist || []).map((item, key) => (
                    <SwiperSlide key={key}>
                        <Card>
                            <CardBody>
                                <div className="float-end">
                                    <UncontrolledDropdown direction='start'>
                                        <DropdownToggle
                                            tag="a"
                                            className="text-reset"
                                            role="button"
                                        >
                                            <span className="text-muted fs-18">
                                                <i className="mdi mdi-dots-horizontal"></i>
                                            </span>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <DropdownItem>
                                                View Details
                                            </DropdownItem>
                                            <DropdownItem>
                                                Remove Watchlist
                                            </DropdownItem>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                                <div className="d-flex align-items-center">
                                    <img
                                        src={item.img}
                                        className="bg-light rounded-circle p-1 avatar-xs img-fluid"
                                        alt=""
                                    />
                                    <h6 className="ms-2 mb-0 fs-14">{item.coinName}</h6>
                                </div>
                                <Row className="align-items-end g-0">
                                    <Col xs={6}>
                                        <h5 className="mb-1 mt-4">{item.price}</h5>
                                        <p className={"fw-medium mb-0 text-" + item.percentageClass}>
                                            {item.percentage}
                                            <span className="text-muted ms-2 fs-12">({item.coinNameAbbr})</span>
                                        </p>
                                    </Col>
                                    <Col xs={6}>
                                        <div
                                            className="apex-charts crypto-widget"
                                            data-colors='["--vz-success" , "--vz-transparent"]'
                                            id="bitcoin_sparkline_charts"
                                            dir="ltr"
                                        >
                                            <WatchListChart seriesData={item.series} chartsColor={item.chartsColor} />
                                        </div>
                                    </Col>
                                </Row>
                            </CardBody>
                        </Card>
                    </SwiperSlide>
                ))}
            </Swiper>
        </React.Fragment>
    );
};

export default Watchlist;