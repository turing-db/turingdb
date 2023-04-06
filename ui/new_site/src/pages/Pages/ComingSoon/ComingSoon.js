import React from 'react'
import { Col, Container, Input, InputGroup, Row } from 'reactstrap'
import ParticlesAuth from "../../../pages/AuthenticationInner/ParticlesAuth";
import Countdown from "react-countdown"

//import images
import comingsoon from '../../../assets/images/comingsoon.png';

const ComingSoon = () => {
  document.title="Coming Soon | Velzon - React Admin & Dashboard Template";

    const renderer = ({ days, hours, minutes, seconds, completed }) => {
        if (completed) {
            // Render a completed state
            return <span>You are good to go!</span>
        } else {
            return (
                <>
                    <div className="countdownlist">
                        <div className="countdownlist-item">
                            <div className="count-title">Days</div>
                            <div className="count-num">{days}</div></div>
                        <div className="countdownlist-item">
                            <div className="count-title">Hours</div>
                            <div className="count-num">{hours}</div></div>
                        <div className="countdownlist-item"><div className="count-title">Minutes</div>
                            <div className="count-num">{minutes}</div></div><div className="countdownlist-item">
                            <div className="count-title">Seconds</div>
                            <div className="count-num">{seconds}</div></div>
                    </div>
                </>
            )
        }
    }

    return (
        <React.Fragment>           
            <ParticlesAuth>
                <div className="auth-page-content">
                    <Container>
                        <Row>
                            <Col lg={12}>
                                <div className="text-center mt-sm-5 pt-4 mb-4">
                                    <div className="mb-sm-5 pb-sm-4 pb-5">
                                        <img src={comingsoon} alt="" height="120" className="move-animation" />
                                    </div>
                                    <div className="mb-5">
                                        <h1 className="display-2 coming-soon-text">Coming Soon</h1>
                                    </div>
                                    <div>
                                        <Row className="justify-content-center mt-5">
                                            <Col lg={8}>
                                                <Countdown date="2022/12/31" renderer={renderer} />
                                            </Col>
                                        </Row>

                                        <div className="mt-5">
                                            <h4>Get notified when we launch</h4>
                                            <p className="text-muted">Don't worry we will not spam you 😊</p>
                                        </div>

                                        <InputGroup className="countdown-input-group mx-auto my-4">
                                            <Input type="email" className="form-control border-light shadow" placeholder="Enter your email address" />
                                            <button className="btn btn-success" type="button" >Send
                                                <i className="ri-send-plane-2-fill align-bottom ms-2"></i>
                                            </button>
                                        </InputGroup>
                                    </div>
                                </div>
                            </Col>
                        </Row>

                    </Container>

                </div>
            </ParticlesAuth>
        </React.Fragment>
    )
}

export default ComingSoon