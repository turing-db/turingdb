import React from "react";
import { Col, Container, Row } from "reactstrap";
import BreadCrumb from "../../Components/Common/BreadCrumb";

import Widgets from "./Widgets";
import Marketplace from "./Marketplace";
import Popularity from "./Popularity";
import FeaturedNFT from "./FeaturedNFT";
import RecentNFTs from "./RecentNFTs";

const DashboardNFT = () => {
    document.title = "NFT Dashboard | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="NFT Dashboard" pageTitle="Dashboard" />
                    <Row className="dash-nft">
                        <Col xxl={9}>
                            <Widgets />
                            <Marketplace />
                        </Col>
                        <Popularity />
                    </Row>
                    <FeaturedNFT />
                    <RecentNFTs />
                </Container>
            </div>
        </React.Fragment>
    );
};

export default DashboardNFT;
