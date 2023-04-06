import React, { useState } from 'react';
import { Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import { Swiper, SwiperSlide } from "swiper/react";

import { Link } from 'react-router-dom';
import SwiperCore, { FreeMode, Navigation, Thumbs } from "swiper";

// Import Swiper styles
import "swiper/css";
import "swiper/css/free-mode";
import "swiper/css/navigation";
import "swiper/css/thumbs";
//import Images
import avatar5 from "../../../assets/images/users/avatar-5.jpg"; 
import avatar4 from "../../../assets/images/users/avatar-4.jpg"; 
import avatar3 from "../../../assets/images/users/avatar-3.jpg"; 
import avatar7 from "../../../assets/images/users/avatar-7.jpg"; 

import small2 from "../../../assets/images/small/img-2.jpg";
import small3 from "../../../assets/images/small/img-3.jpg";
import small4 from "../../../assets/images/small/img-4.jpg";
import small6 from "../../../assets/images/small/img-6.jpg";
import small7 from "../../../assets/images/small/img-7.jpg";
import small10 from "../../../assets/images/small/img-10.jpg";
import small9 from "../../../assets/images/small/img-9.jpg";


const Timeline = () => {
  document.title="Timeline | Velzon - React Admin & Dashboard Template";
    
    SwiperCore.use([FreeMode, Navigation, Thumbs]);
  const [thumbsSwiper] = useState(null);

    return (
        <React.Fragment>
            <div className="page-content">                
                <Container fluid>
                <BreadCrumb title="Timeline" pageTitle="Pages" />       
                    <Row>
                        <Col lg={12}>
                            <div>
                                <h5>Center Timeline</h5>
                                <div className="timeline">
                                    <div className="timeline-item left">
                                        <i className="icon ri-stack-line"></i>
                                        <div className="date">15 Dec 2021</div>
                                        <div className="content">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar5} alt="" className="avatar-sm rounded"/>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h5>@Erica245 <small className="text-muted fs-13 fw-normal">- 10 min Ago</small></h5>
                                                    <p className="text-muted mb-2">Wish someone a sincere ‘good luck in your new job’ with these sweet messages. Make sure you pick out a good luck new job card to go with the words, and pop a beautiful bunch of flowers from our gift range in your basket, to make them feel super special.</p>
                                                    <div className="hstack gap-2">
                                                        <Link to="#" className="btn btn-sm btn-light"><span className="me-1">&#128293;</span> 19</Link>
                                                        <Link to="#" className="btn btn-sm btn-light"><span className="me-1">&#129321;</span> 22</Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="timeline-item right">
                                        <i className="icon ri-vip-diamond-line"></i>
                                        <div className="date">22 Oct 2021</div>
                                        <div className="content">
                                            <h5>Adding a new event with attachments</h5>
                                            <p className="text-muted">Too much or too little spacing, as in the example below, can make things unpleasant for the reader.</p>
                                            <Row className="g-2">
                                                <Col sm={6}>
                                                    <div className="d-flex border border-dashed p-2 rounded position-relative">
                                                        <div className="flex-shrink-0 avatar-xs">
                                                            <div className="avatar-title bg-soft-danger text-danger fs-15 rounded">
                                                                <i className="ri-image-2-line"></i>
                                                            </div>
                                                        </div>
                                                        <div className="flex-shrink-0">

                                                        </div>
                                                        <div className="flex-grow-1 overflow-hidden ms-2">
                                                            <h6 className="text-truncate mb-0"><Link to="#" className="stretched-link">Business Template - UI/UX design</Link></h6>
                                                            <small>685 KB</small>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col sm={6}>
                                                    <div className="d-flex border border-dashed p-2 rounded position-relative">
                                                        <div className="flex-shrink-0 avatar-xs">
                                                            <div className="avatar-title bg-soft-info text-info fs-15 rounded">
                                                                <i className="ri-file-zip-line"></i>
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1 ms-2 overflow-hidden">
                                                            <h6 className="mb-0 text-truncate"><Link to="#" className="stretched-link">Bank Management System - PSD</Link></h6>
                                                            <small>8.78 MB</small>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="timeline-item left">
                                        <i className="icon ri-gift-line"></i>
                                        <div className="date">10 Jul 2021</div>
                                        <div className="content">
                                            <h5>Create new project buildng product</h5>
                                            <p className="text-muted">Every team project can have a velzon. Use the velzon to share information with your team to understand and contribute to your project.</p>
                                            <div className="avatar-group mb-2">
                                                <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title="Christi">
                                                    <img src={avatar4} alt="" className="rounded-circle avatar-xs"/>
                                                </Link>
                                                <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title="Frank Hook">
                                                    <img src={avatar3} alt="" className="rounded-circle avatar-xs"/>
                                                </Link>
                                                <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title=" Ruby">
                                                    <div className="avatar-xs">
                                                        <div className="avatar-title rounded-circle bg-light text-primary">
                                                            R
                                                        </div>
                                                    </div>
                                                </Link>
                                                <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title="more">
                                                    <div className="avatar-xs">
                                                        <div className="avatar-title rounded-circle">
                                                            2+
                                                        </div>
                                                    </div>
                                                </Link>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="timeline-item right">
                                        <i className="icon ri-shield-star-line"></i>
                                        <div className="date">18 May 2021</div>
                                        <div className="content">
                                            <h5>Donald Palmer <small className="text-muted fs-14 fw-normal">- Has changed 2 attributes</small></h5>
                                            <p className="text-muted fst-italic mb-2">" This is an awesome admin dashboard template. It is extremely well structured and uses state of the art components (e.g. one of the only templates using boostrap 5.1.3 so far). I integrated it into a Rails 6 project. Needs manual integration work of course but the template structure made it easy. "</p>
                                            <div className="hstack gap-2">
                                                <Link to="#" className="btn btn-sm bg-light"><span className="me-1">&#128151;</span> 35</Link>
                                                <Link to="#" className="btn btn-sm btn-light"><span className="me-1">&#128077;</span> 10</Link>
                                                <Link to="#" className="btn btn-sm btn-light"><span className="me-1">&#128591;</span> 10</Link>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="timeline-item left">
                                        <i className="icon ri-user-smile-line"></i>
                                        <div className="date">10 Feb 2021</div>
                                        <div className="content">
                                            <h5>Velzon admin dashboard templates layout upload</h5>
                                            <p className="text-muted">Powerful, clean & modern responsive bootstrap 5 admin template. The maximum file size for uploads in this demo :</p>
                                            <Row className="border border-dashed rounded gx-2 p-2">
                                                <Col xs={3}>
                                                    <img src={small2} alt="" className="img-fluid rounded" />
                                                </Col>
                                                <Col xs={3}>
                                                    <img src={small3} alt="" className="img-fluid rounded" />
                                                </Col>
                                                <Col xs={3}>
                                                    <img src={small4} alt="" className="img-fluid rounded" />
                                                </Col>
                                                <Col xs={3}>
                                                    <img src={small6} alt="" className="img-fluid rounded" />
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="timeline-item right">
                                        <i className="icon ri-fire-line"></i>
                                        <div className="date">01 Jan 2021</div>
                                        <div className="content">
                                            <h5>New ticket received <span className="badge bg-soft-success text-success fs-11 align-middle ms-1">Completed</span></h5>
                                            <p className="text-muted mb-2">
                                                It is important for us that we receive email notifications when a ticket is created as our IT staff are mobile and will not always be looking at the dashboard for new tickets.
                                            </p>
                                            <Link to="#" className="link-primary text-decoration-underline">Read More <i className="ri-arrow-right-line"></i></Link>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </Col>
                    </Row>

                    <Row className="mt-4">
                        <Col lg={12}>
                            <div>
                                <h5 className="mb-4">Left Timeline</h5>
                                <div className="timeline-2">
                                    <div className="timeline-year">
                                        <p>12 Dec 2021</p>
                                    </div>
                                    <div className="timeline-continue">
                                        <Row className="timeline-right">
                                            <Col xs={12}>
                                                <p className="timeline-date">
                                                    02:35AM to 04:30PM
                                                </p>
                                            </Col>
                                            <Col xs={12}>
                                                <div className="timeline-box">
                                                    <div className="timeline-text">
                                                        <div className="d-flex">
                                                            <img src={avatar7} alt="" className="avatar-sm rounded" />
                                                            <div className="flex-grow-1 ms-3">
                                                                <h5 className="mb-1">Frank hook joined with our company</h5>
                                                                <p className="text-muted mb-0">It makes a statement, it’s impressive graphic design. Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each letter has the perfect spot of its own. </p>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row className="timeline-right">
                                            <Col xs={12}>
                                                <p className="timeline-date">
                                                    02:35AM to 04:30PM
                                                </p>
                                            </Col>
                                            <Col xs={12}>
                                                <div className="timeline-box">
                                                    <div className="timeline-text">
                                                        <h5>Trip planning</h5>
                                                        <p className="text-muted">In the trip planner, keep only one or two activities in a day if the purpose of the trip is to relax and take it easy during the vacation :</p>
                                                        <Row className="border border-dashed rounded gx-2 p-2">
                                                            <Col xs={3}>
                                                                <Link to="#"><img src={small7} alt="" className="img-fluid rounded" /></Link>
                                                            </Col>
                                                            <Col xs={3}>
                                                                <Link to="#"><img src={small3} alt="" className="img-fluid rounded" /></Link>
                                                            </Col>
                                                            <Col xs={3}>
                                                                <Link to="#"><img src={small10} alt="" className="img-fluid rounded" /></Link>
                                                            </Col>
                                                            <Col xs={3}>
                                                                <Link to="#"><img src={small9} alt="" className="img-fluid rounded" /></Link>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row>
                                            <Col xs={12}>
                                                <div className="timeline-year">
                                                    <p>08 Dec 2021</p>
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row className="timeline-right">
                                            <Col xs={12}>
                                                <p className="timeline-date">
                                                    02:35AM to 04:30PM
                                                </p>
                                            </Col>
                                            <Col xs={12}>
                                                <div className="timeline-box">
                                                    <div className="timeline-text">
                                                        <h5>Velzon - Project Discussion</h5>
                                                        <p className="text-muted mb-0">The purpose of the discussion is to interpret and describe the significance of your findings in light of what was already known about the research problem being investigated, and to explain any new understanding or fresh insights about the problem after you've taken the findings into consideration.</p>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row className="timeline-right">
                                            <Col xs={12}>
                                                <p className="timeline-date">
                                                    10:24AM to 11:15PM
                                                </p>
                                            </Col>
                                            <Col xs={12}>
                                                <div className="timeline-box">
                                                    <div className="timeline-text">
                                                        <h5>Client Meeting (Nancy Martino)</h5>
                                                        <p className="text-muted mb-0">A client meeting, meaning, direct collaboration and communication with a customer, is the best way to understand their needs and how you can help support them.</p>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row className="timeline-right">
                                            <Col xs={12}>
                                                <p className="timeline-date">
                                                    9:20AM to 10:05PM
                                                </p>
                                            </Col>
                                            <Col xs={12}>
                                                <div className="timeline-box">
                                                    <div className="timeline-text">
                                                        <h5>Designer & Developer Meeting</h5>
                                                        <ul className="mb-0 text-muted vstack gap-2">
                                                            <li>Last updates</li>
                                                            <li>Weekly Planning</li>
                                                            <li>Work Distribution</li>
                                                        </ul>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="timeline-year">
                                        <p><span>05 Dec 2021</span></p>
                                    </div>
                                    <div className="timeline-launch">
                                        <div className="timeline-box">
                                            <div className="timeline-text">
                                                <h5>Our Company Activity</h5>
                                                <p className="text-muted text-capitalize mb-0">Wow...!!! What a Journey So Far...!!!</p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </Col>
                    </Row>
                    <Row className="mt-4">
                        <Col lg={12}>
                            <div>
                                <h5>Horizontal Timeline</h5>
                                <div className="horizontal-timeline my-3">
                                <Swiper className="timelineSlider"
                                        navigation={true}
                                        thumbs={{ swiper: thumbsSwiper }}
                                        slidesPerView={5}
                                    >                                      
                                        <div className="swiper-wrapper">
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted fw-medium mb-1">December, 2020</p>
                                                            <h6 className="mb-0 fs-15">Plateform Development</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">10 : 35 PM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">January, 2021</p>
                                                            <h6 className="mb-0 fs-15">Release Bank & Cards Phase</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">8 : 10 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">March, 2021</p>
                                                            <h6 className="mb-0 fs-15">Beta Launch of Plateform</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">01 : 47 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">April, 2021</p>
                                                            <h6 className="mb-0 fs-15">First Crypto Bank Transfers</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">07 : 53 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">May, 2021</p>
                                                            <h6 className="mb-0 fs-15">Launch Ethbay Services</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">4 : 05 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">June, 2021</p>
                                                            <h6 className="mb-0 fs-15">Fastest Crypto Transaction</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">02 : 40 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">July, 2021</p>
                                                            <h6 className="mb-0 fs-15">Crypto Blockchain Release</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">9 : 27 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                            <SwiperSlide>
                                                <div className="card pt-2 border-0 item-box text-center">
                                                    <div className="timeline-content p-3 rounded">
                                                        <div>
                                                            <p className="text-muted mb-1">August, 2021</p>
                                                            <h6 className="mb-0 fs-15">Launch Ethereum Classifieds</h6>
                                                        </div>
                                                    </div>
                                                    <div className="time"><span className="badge bg-success">06 : 19 AM</span></div>
                                                </div>
                                            </SwiperSlide>
                                        </div>
                                        {/* <div className="swiper-button-next"></div>
                                        <div className="swiper-button-prev"></div> */}
                                    </Swiper>
                                </div>
                            </div>
                        </Col>
                    </Row>

                </Container>
                
            </div>
        </React.Fragment>
    )
}

export default Timeline

