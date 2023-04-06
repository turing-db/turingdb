import React from "react";
import { Card, CardBody, Col, Container, Row } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

// Import Images
import metaMask from "../../../assets/images/nft/wallet/metamask.png";

import { walletConnectData } from "../../../common/data/index";

const WalletConnect = () => {
    document.title = "Wallet Connect | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Wallet Connect" pageTitle="NFT Marketplace" />
                    <Row className="justify-content-center">
                        <Col xl={5}>
                            <div className="text-center mb-4">
                                <h4>Your current wallet</h4>
                                <p className="text-muted fs-13">WalletConnect is a convenient open source tool that enables a mobile wallet to easily connect to decentralized web applications, and interact with them from your phone.</p>
                            </div>
                        </Col>
                    </Row>

                    <Row className="justify-content-center mb-4">
                        <Col lg={3}>
                            <Card className="text-center">
                                <CardBody className="py-5 px-4">
                                    <img src={metaMask} alt="" height="55" className="mb-3 pb-2" />
                                    <h5>Metamask</h5>
                                    <p className="text-muted pb-1">MetaMask is a software cryptocurrency wallet used to interact with the Ethereum blockchain.</p>
                                    <Link to="#!" className="btn btn-danger">Change Wallet</Link>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row className="justify-content-center">
                        <Col xl={5}>
                            <div className="text-center mb-4">
                                <h4>Connect with one of our other available wallet providers.</h4>
                            </div>
                        </Col>
                    </Row>

                    <Row className="justify-content-center">
                        {walletConnectData.map((item, key) => (
                            <Col key={key} lg={3}>
                                <Card className="text-center">
                                    <CardBody className="py-5 px-4">
                                        <img src={item.img} alt="" height="55" className="mb-3 pb-2" />
                                        <h5>{item.title}</h5>
                                        <p className="text-muted pb-1">{item.textContent}</p>
                                        <Link to="#!" className="btn btn-soft-info">Connect Wallet</Link>
                                    </CardBody>
                                </Card>
                            </Col>
                        ))}
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default WalletConnect;