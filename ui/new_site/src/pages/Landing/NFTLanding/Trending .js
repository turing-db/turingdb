import React from 'react';
import { Container, Row, Col, Card, CardBody } from 'reactstrap';
import { Link } from 'react-router-dom';

//swiper
import { Swiper, SwiperSlide } from "swiper/react";
import "swiper/css";
import "swiper/css/pagination";
import { Pagination, Autoplay, Navigation } from "swiper";

//import Images
import Img01 from "../../../assets/images/nft/img-01.jpg";
import Img02 from "../../../assets/images/nft/img-02.jpg";
import Img03 from "../../../assets/images/nft/img-03.jpg";
import Img04 from "../../../assets/images/nft/img-04.jpg";
import Img05 from "../../../assets/images/nft/img-05.jpg";
import Img06 from "../../../assets/images/nft/img-06.jpg";

// import GifImg01 from "../../../assets/images/nft/gif/img-1.gif";
// import GifImg02 from "../../../assets/images/nft/gif/img-2.gif";
// import GifImg03 from "../../../assets/images/nft/gif/img-3.gif";
// import GifImg04 from "../../../assets/images/nft/gif/img-4.gif";
// import GifImg05 from "../../../assets/images/nft/gif/img-5.gif";

const Trending = () => {
    return (
        <React.Fragment>
            <section className="section bg-light" id="categories">
                <Container fluid>
                    <Row className="justify-content-center">
                        <Col lg={5}>
                            <div className="text-center mb-5">
                                <h2 className="mb-3 fw-semibold lh-base">Trending All Categories</h2>
                                <p className="text-muted">The process of creating an NFT may cost less than a dollar, but the process of selling it can cost up to a thousand dollars. For example, Allen Gannett, a software developer.</p>
                            </div>
                        </Col>
                    </Row>
                    <Row>
                        <Col lg={12}>
                            <Swiper modules={[Navigation, Autoplay, Pagination]}
                                slidesPerView={4}
                                spaceBetween={30}
                                navigation={{
                                    nextEl: ".swiper-button-next",
                                    prevEl: ".swiper-button-prev",
                                }}
                                pagination={{
                                    el: ".swiper-pagination",
                                    clickable: true,
                                }}
                                breakpoints={{
                                    640: {
                                        slidesPerView: 2,
                                        spaceBetween: 20,
                                    },
                                    768: {
                                        slidesPerView: 3,
                                        spaceBetween: 24,
                                    },
                                    1024: {
                                        slidesPerView: 4,
                                        spaceBetween: 30,
                                    },
                                }}
                                loop={true}
                                autoplay={{ delay: 2500, disableOnInteraction: false }} 
                                className="mySwiper swiper mySwiper pb-4">
                                <div className="swiper-wrapper">
                                    <SwiperSlide>
                                        <Card>
                                            <CardBody>
                                                <Row className="g-1 mb-3">
                                                    <Col lg={6}>
                                                        <img src={Img06} alt="" className="img-fluid rounded" />
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-2.gif"} alt="" className="img-fluid rounded mt-1" />
                                                    </Col>
                                                    <Col lg={6}>
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-5.gif"} alt="" className="img-fluid rounded mb-1" />
                                                        <img src={Img03} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                </Row>
                                                <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                                <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">Artwork <span className="badge badge-soft-success">206</span></Link></h5>
                                            </CardBody>
                                        </Card>
                                    </SwiperSlide>
                                    <SwiperSlide>
                                        <Card>
                                            <CardBody>
                                                <Row className="g-1 mb-3">
                                                    <Col lg={6}>
                                                        <img src={Img05} alt="" className="img-fluid rounded" />
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-1.gif"} alt="" className="img-fluid rounded mt-1" />
                                                    </Col>
                                                    <Col lg={6}>
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-4.gif"} alt="" className="img-fluid rounded mb-1" />
                                                        <img src={Img04} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                </Row>
                                                <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                                <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">Crypto Card <span className="badge badge-soft-success">743</span></Link></h5>
                                            </CardBody>
                                        </Card>
                                    </SwiperSlide>
                                    <SwiperSlide>
                                        <Card>
                                            <CardBody>
                                                <Row className="g-1 mb-3">
                                                    <Col lg={6}>
                                                        <img src={Img02} alt="" className="img-fluid rounded" />
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-3.gif"} alt="" className="img-fluid rounded mt-1" />
                                                    </Col>
                                                    <Col lg={6}>
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-1.gif"} alt="" className="img-fluid rounded mb-1" />
                                                        <img src={Img01} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                </Row>
                                                <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                                <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">Music <span className="badge badge-soft-success">679</span></Link></h5>
                                            </CardBody>
                                        </Card>
                                    </SwiperSlide>
                                    <SwiperSlide>
                                        <Card>
                                            <CardBody>
                                                <Row className="g-1 mb-3">
                                                    <Col lg={6}>
                                                        <img src={Img03} alt="" className="img-fluid rounded" />
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-5.gif"} alt="" className="img-fluid rounded mt-1" />
                                                    </Col>
                                                    <Col lg={6}>
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-2.gif"} alt="" className="img-fluid rounded mb-1" />
                                                        <img src={Img05} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                </Row>
                                                <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                                <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">Games <span className="badge badge-soft-success">341</span></Link></h5>
                                            </CardBody>
                                        </Card>
                                    </SwiperSlide>
                                    <SwiperSlide>
                                        <Card>
                                            <CardBody>
                                                <Row className="g-1 mb-3">
                                                    <Col lg={6}>
                                                        <img src={Img02} alt="" className="img-fluid rounded" />
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-3.gif"} alt="" className="img-fluid rounded mt-1" />
                                                    </Col>
                                                    <Col lg={6}>
                                                        <img src={"https://img.themesbrand.com/velzon/images/img-1.gif"} alt="" className="img-fluid rounded mb-1" />
                                                        <img src={Img01} alt="" className="img-fluid rounded" />
                                                    </Col>
                                                </Row>
                                                <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                                <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">Photography <span className="badge badge-soft-success">1452</span></Link></h5>
                                            </CardBody>
                                        </Card>
                                    </SwiperSlide>
                                </div>
                                <div className="swiper-pagination swiper-pagination-dark"></div>
                            </Swiper>
                        </Col>
                    </Row>
                </Container>
            </section>
        </React.Fragment>
    );
};

export default Trending;