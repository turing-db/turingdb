import React from 'react';
import { Container, Row, Col, Card, CardBody, UncontrolledDropdown, DropdownToggle, DropdownMenu, DropdownItem} from 'reactstrap';
import { Link } from 'react-router-dom';
import { topCreatorData } from '../../../common/data';

const TopCreator = () => {
    
    return (
        <React.Fragment>
            <section className="section bg-light" id="creators">
                <Container>
                    <Row className="justify-content-center">
                        <Col lg={8}>
                            <div className="text-center mb-5">
                                <h2 className="mb-3 fw-semibold lh-base">Top Creator This Week</h2>
                                <p className="text-muted">NFTs are valuable because they verify the authenticity of a non-fungible asset. This makes these assets unique and one of a kind.</p>
                            </div>
                        </Col>
                    </Row>
                    <Row>
                    {topCreatorData.map((item, key) => (
                        <Col key={key} xl={4} md={6}>
                            <Card>
                                <CardBody>
                                    <div className="d-flex">
                                        <div className="flex-shink-0">
                                            <img src={item.img} alt="" className="avatar-sm object-cover rounded" />
                                        </div>
                                        <div className="ms-3 flex-grow-1">
                                            <Link to="pages-profile">
                                                <h5 className="mb-1">{item.title}</h5>
                                            </Link>
                                            <p className="text-muted mb-0"><i className="mdi mdi-ethereum text-primary fs-14"></i> {item.price}</p>
                                        </div>
                                        <div>
                                        <UncontrolledDropdown direction="start" className="float-end">
                                                <DropdownToggle tag="button" className="btn btn-ghost-primary btn-icon">
                                                <i className="ri-more-fill align-middle fs-16"></i>
                                                </DropdownToggle>
                                                <DropdownMenu className="dropdown-menu-end">
                                                    <DropdownItem className="view-item-btn">Share</DropdownItem>
                                                    <DropdownItem className="edit-item-btn">Report</DropdownItem>
                                                </DropdownMenu>
                                            </UncontrolledDropdown>
                                        </div>
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
}

export default TopCreator;