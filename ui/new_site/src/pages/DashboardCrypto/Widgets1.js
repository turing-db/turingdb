import React from 'react';
import { Card, CardBody, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from 'reactstrap';
import { Swiper, SwiperSlide } from "swiper/react";

import "swiper/css";
import "swiper/css/pagination";

// import required modules
import { Autoplay, Mousewheel } from "swiper";
import { cryptoSlider } from '../../common/data';
import { WidgetsCharts } from './DashboardCryptoCharts';

const Widgets1 = () => {
    return (
        <React.Fragment>
            <Col lg={12}>
                <Swiper
                    slidesPerView={5}
                    spaceBetween={24}
                    mousewheel={true}
                    autoplay={{
                        delay: 2000,
                        disableOnInteraction: false,
                    }}
                    modules={[Autoplay, Mousewheel]}
                    className="cryptoSlider">

                    {(cryptoSlider || []).map((item, key) => (
                        <SwiperSlide key={key}>
                            <Card>
                                <CardBody>
                                    <div className="float-end">
                                        <UncontrolledDropdown direction='start'>
                                            <DropdownToggle className="text-reset" tag="a" role="button">
                                                <span className="text-muted fs-18"><i className="mdi mdi-dots-horizontal"></i></span>
                                            </DropdownToggle>
                                            <DropdownMenu className="dropdown-menu dropdown-menu-end">
                                                <DropdownItem href="#"> Details </DropdownItem>
                                                <DropdownItem href="#"> Cancel </DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>
                                    </div>
                                    <div className="d-flex align-items-center">
                                        <img src={item.img} className="bg-light rounded-circle p-1 avatar-xs img-fluid" alt="" />
                                        <h6 className="ms-2 mb-0">{item.label}</h6>
                                    </div>
                                    <Row className="align-items-end g-0">
                                        <Col xs={6}>
                                            <h5 className="mb-1 mt-4">{item.price}</h5>
                                            <p className={"fw-medium mb-0 text-" + item.changeClass}>{item.change}<span className="text-muted ms-2 text-uppercase">({item.coinName})</span></p>
                                        </Col>
                                        <Col xs={6}>
                                            <div className="apex-charts crypto-widget" dir="ltr">
                                                <WidgetsCharts seriesData={item.series} chartsColor={item.chartsColor} />
                                            </div>
                                        </Col>
                                    </Row>
                                </CardBody>
                            </Card>
                        </SwiperSlide>
                    ))}
                </Swiper>
            </Col>
        </React.Fragment>
    );
};

export default Widgets1;