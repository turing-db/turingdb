import React from 'react';
import { Container, Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

const Home = () => {
    return (
        <React.Fragment>
             <section className="section nft-hero" id="hero">
            <div className="bg-overlay"></div>
            <Container>
                <Row className="justify-content-center">
                    <Col lg={8} sm={10}>
                        <div className="text-center">
                            <h1 className="display-4 fw-medium mb-4 lh-base text-white">Discover Digital Art & Collect <span className="text-success">NFT Marketplace</span></h1>
                            <p className="lead text-white-50 lh-base mb-4 pb-2">Can artwork be NFT? NFTs (non-fungible tokens) are one-of-a-kind digital assets. Given they're digital in nature, can physical works of art be turned into NFTs?.</p>

                            <div className="hstack gap-2 justify-content-center">
                                <Link to="/nft-create" className="btn btn-primary">Create Own <i className="ri-arrow-right-line align-middle ms-1"></i></Link>
                                <Link to="/nft-explore" className="btn btn-danger">Explore Now <i className="ri-arrow-right-line align-middle ms-1"></i></Link>
                            </div>
                        </div>
                    </Col>
                </Row>
            </Container>
        </section>
        </React.Fragment>
    );
}

export default Home;