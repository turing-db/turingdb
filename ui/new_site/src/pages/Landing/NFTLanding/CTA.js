import React from 'react';
import { Container, Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

const CTA = () => {
    return (
        <React.Fragment>
            <section className="py-5 bg-primary position-relative">
                <div className="bg-overlay bg-overlay-pattern opacity-50"></div>
                <Container>
                    <Row className="align-items-center gy-4">
                        <Col className="col-sm">
                            <div>
                                <h4 className="text-white mb-0 fw-semibold">Create and Sell Your NFT's</h4>
                            </div>
                        </Col>

                        <Col className="col-sm-auto">
                            <div>
                                <Link to="/apps-nft-create" className="btn bg-gradient btn-danger me-1">Create NFT</Link>
                                <Link to="/apps-nft-explore" className="btn bg-gradient btn-info">Discover More</Link>
                            </div>
                        </Col>
                    </Row>
                </Container>
            </section>
        </React.Fragment>
    );
}

export default CTA;