import React from 'react';
import { Card, CardBody, Col, Container, Row } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Images
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";
import avatar10 from "../../../assets/images/users/avatar-10.jpg";


const Team = () => {
    return (
        <React.Fragment>
            <section className="section bg-light" id="team">
                <Container>
                    <Row className="justify-content-center">
                        <Col lg={8}>
                            <div className="text-center mb-5">
                                <h3 className="mb-3 fw-semibold">Our <span className="text-danger">Team</span></h3>
                                <p className="text-muted mb-4 ff-secondary">To achieve this, it would be necessary to have uniform grammar,
                                    pronunciation and more common words. If several languages coalesce the grammar.</p>
                            </div>
                        </Col>
                    </Row>
                    <Row>
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar2} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Nancy Martino</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">Team Leader</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar10} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Henry Baird</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">Full Stack Developer</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar3} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Frank Hook</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">Project Manager</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar8} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Donald Palmer</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">UI/UX Designer</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                    </Row>

                    {/* <!-- end row --> */}
                    <Row>
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar5} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Erica Kernan</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">Web Designer</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar4} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Alexis Clarke</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">Backend Developer</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar6} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Marie Ward</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">Full Stack Developer</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}

                        <Col lg={3} sm={6}>
                            <Card>
                                <CardBody className="text-center p-4">
                                    <div className="avatar-xl mx-auto mb-4 position-relative">
                                        <img src={avatar7} alt="" className="img-fluid rounded-circle" />
                                        <Link to="/apps-mailbox"
                                            className="btn btn-success btn-sm position-absolute bottom-0 end-0 rounded-circle avatar-xs">
                                            <div className="avatar-title bg-transparent">
                                                <i className="ri-mail-fill align-bottom"></i>
                                            </div>
                                        </Link>
                                    </div>

                                    {/* <!-- end card body --> */}
                                    <h5 className="mb-1"><Link to="/pages-profile" className="text-body">Jack Gough</Link></h5>
                                    <p className="text-muted mb-0 ff-secondary">React Js Developer</p>
                                </CardBody>
                            </Card>

                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                    </Row>

                    {/* <!-- end row --> */}
                    <Row>
                        <div className="col-lg-12">
                            <div className="text-center mt-2">
                                <Link to="/pages-team" className="btn btn-primary">View All Members <i
                                    className="ri-arrow-right-line ms-1 align-bottom"></i></Link>
                            </div>
                        </div>
                    </Row>

                    {/* <!-- end row --> */}
                </Container>
            </section>
        </React.Fragment>
    );
};

export default Team;