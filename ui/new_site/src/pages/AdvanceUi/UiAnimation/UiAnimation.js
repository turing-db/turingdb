import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Images
import macImg from "../../../assets/images/mac-img.png";

import AOS from 'aos';
import 'aos/dist/aos.css';
import { FadeUpExample, FadeDownExample, FadeRightExample, FadeLeftExample, FadeUpRightExample, FadeUpLeftExample, FadeDownRightExample, FadeDownLeftExample, FlipLeftExample, FlipRightExample, FilpUpExample, FilpDownExample, ZoomInExample, ZoomOutExample, ZoomInUpExample, ZoomOutUpExample, ZoomInDownExample, ZoomOutDownExample, ZoomInRightExample, ZoomOutRightExample, ZoomInLeftExample, ZoomOutLeftExample, DurationExample, EasingLinearExample, Offset1Example, Offset2Example, EaseInBackExample, EaseOutCubicExample } from './UiAnimationCode';

AOS.init({
    easing: 'ease-out-back',
    duration: 3000,
    anchorPlacement: 'top-bottom', 
});
const UiAnimation = () => {
    document.title="Animation | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Animation" pageTitle="Advance UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <div className="text-center">
                                        <h4 className="card-title mb-0">Fade Animation</h4>
                                    </div>
                                </CardHeader>

                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col lg={6} >
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Up</h4>
                                                        <pre>
                                                            <code className="language-markup">
                                                                <FadeUpExample />
                                                            </code>
                                                        </pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} lg={8}>
                                                                <div data-aos="fade-up">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Down</h4>
                                                        <pre><code className="language-markup"><FadeDownExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} lg={8}>
                                                                <div data-aos="fade-down">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Right</h4>
                                                        <pre><code className="language-markup"><FadeRightExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-right">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Left</h4>
                                                        <pre><code className="language-markup"><FadeLeftExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-left">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Up Right</h4>
                                                        <pre><code className="language-markup"><FadeUpRightExample /></code></pre>
                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-up-right">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Up Left</h4>
                                                        <pre><code className="language-markup"><FadeUpLeftExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-up-left">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>
                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Down Right</h4>
                                                        <pre><code className="language-markup"><FadeDownRightExample /></code></pre>
                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-down-right">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Fade Down Left</h4>
                                                        <pre><code className="language-markup"><FadeDownLeftExample /></code></pre>
                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-down-left">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <div className="text-center">
                                        <h4 className="card-title mb-0">Flip Animation</h4>
                                    </div>
                                </CardHeader>

                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Flip Left</h4>
                                                        <pre><code className="language-markup"><FlipLeftExample /></code></pre>
                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="flip-left">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Flip Right</h4>
                                                        <pre><code className="language-markup"><FlipRightExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="flip-right">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Flip Up</h4>
                                                        <pre><code className="language-markup"><FilpUpExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="flip-up">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Flip Down</h4>
                                                        <pre><code className="language-markup"><FilpDownExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="flip-down">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <div className="text-center">
                                        <h4 className="card-title mb-0">Zoom Animation</h4>
                                    </div>
                                </CardHeader>

                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom In</h4>
                                                        <pre><code className="language-markup"><ZoomInExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-in">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom In Up</h4>
                                                        <pre><code className="language-markup"><ZoomInUpExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-in-up">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom In Down</h4>
                                                        <pre><code className="language-markup"><ZoomInDownExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-in-down">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom In Right</h4>
                                                        <pre><code className="language-markup"><ZoomInRightExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-in-right">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom In Left</h4>
                                                        <pre><code className="language-markup"><ZoomInLeftExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-in-left">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom Out</h4>
                                                        <pre><code className="language-markup"><ZoomOutExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-out">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom Out Up</h4>
                                                        <pre><code className="language-markup"><ZoomOutUpExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-out-up">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom Out Down</h4>
                                                        <pre><code className="language-markup"><ZoomOutDownExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-out-down">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom Out Right</h4>
                                                        <pre><code className="language-markup"><ZoomOutRightExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-out-right">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>

                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Zoom Out Left</h4>
                                                        <pre><code className="language-markup"><ZoomOutLeftExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="zoom-out-left">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <div className="text-center">
                                        <h4 className="card-title mb-0">Animation Examples</h4>
                                    </div>
                                </CardHeader>
                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Duration</h4>
                                                        <pre><code className="language-markup"><DurationExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-up" data-aos-duration="3000">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Easing="linear"</h4>
                                                        <pre><code className="language-markup"><EasingLinearExample /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-down" data-aos-easing="linear" data-aos-duration="1500">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Offset</h4>
                                                        <pre><code className="language-markup"><Offset1Example /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-right" data-aos-offset="300" data-aos-easing="ease-in-sine">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Offset</h4>
                                                        <pre><code className="language-markup"><Offset2Example /></code></pre>

                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-left" data-aos-offset="500" data-aos-duration="500">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>

                                            <Row>
                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Easing="ease-in-back"</h4>
                                                        <pre><code className="language-markup"><EaseInBackExample /></code></pre>
                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="fade-zoom-in" data-aos-easing="ease-in-back" data-aos-delay="300" data-aos-offset="0">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div className="p-4 pb-0 border mt-4">
                                                        <h4 className="mb-4 text-center">Easing="ease-out-cubic"</h4>
                                                        <pre><code className="language-markup"><EaseOutCubicExample /></code></pre>
                                                        <Row className="justify-content-center">
                                                            <Col sm={10} md={8}>
                                                                <div data-aos="flip-left" data-aos-easing="ease-out-cubic" data-aos-duration="2000">
                                                                    <img src={macImg} alt="Mac " className="img-fluid" />
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default UiAnimation;