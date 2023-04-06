import React, { useState, useEffect } from 'react';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from 'reactstrap';
import CountUp from "react-countup";
import Countdown from "react-countdown";
import { useSelector, useDispatch } from "react-redux";
import { getMarketChartsDatas } from '../../slices/thunks';

// Import Images
// import ImgGif2 from "../../assets/images/nft/gif/img-2.gif";

// Import Chart
import { MarketplaceChart } from "./DashboardNFTCharts";
import { Link } from 'react-router-dom';

const Marketplace = () => {
    const dispatch = useDispatch();

    const [chartData, setchartData] = useState([]);

    const { marketplaceData } = useSelector(state => ({
        marketplaceData: state.DashboardNFT.marketplaceData
    }));

    useEffect(() => {
        setchartData(marketplaceData);
    }, [marketplaceData]);

    const onChangeChartPeriod = pType => {
        dispatch(getMarketChartsDatas(pType));
    };

    useEffect(() => {
        dispatch(getMarketChartsDatas("all"));
    }, [dispatch]);
    const renderer = ({ days, hours, minutes, seconds, completed }) => {
        if (completed) {
            // Render a completed state
            return <span>You are good to go!</span>;
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
            );
        }
    };
    return (
        <React.Fragment>
            <Row>
                <Col xxl={12}>
                    <Card>
                        <CardBody className="p-0">
                            <Row className="g-0">
                                <Col xxl={8}>
                                    <div className="">
                                        <CardHeader className="border-0 align-items-center d-flex">
                                            <h4 className="card-title mb-0 flex-grow-1">Marketplace</h4>
                                            <div className='d-flex gap-1'>
                                                <button type="button" className="btn btn-soft-secondary btn-sm" onClick={() => { onChangeChartPeriod("all"); }}>
                                                    ALL
                                                </button>
                                                <button type="button" className="btn btn-soft-secondary btn-sm" onClick={() => { onChangeChartPeriod("month"); }}>
                                                    1M
                                                </button>
                                                <button type="button" className="btn btn-soft-secondary btn-sm" onClick={() => { onChangeChartPeriod("halfyear"); }}>
                                                    6M
                                                </button>
                                                <button type="button" className="btn btn-soft-primary btn-sm" onClick={() => { onChangeChartPeriod("year"); }}>
                                                    1Y
                                                </button>
                                            </div>
                                        </CardHeader>
                                        <Row className="g-0 text-center">
                                            <Col sm={4} className="col-6">
                                                <div className="p-3 border border-dashed border-start-0">
                                                    <h5 className="mb-1">
                                                        <span className="counter-value" data-target="36.48">
                                                            <CountUp start={0} end={36.48} decimals={2} suffix={"k"} duration={4} />
                                                        </span>
                                                    </h5>
                                                    <p className="text-muted mb-0">Aetworks</p>
                                                </div>
                                            </Col>
                                            <Col sm={4} className="col-6">
                                                <div className="p-3 border border-dashed border-start-0">
                                                    <h5 className="mb-1">
                                                        <span className="counter-value" data-target="92.54">
                                                            <CountUp start={0} end={92.54} decimals={2} suffix={"k"} duration={4} />
                                                        </span>
                                                    </h5>
                                                    <p className="text-muted mb-0">Auction</p>
                                                </div>
                                            </Col>
                                            <Col sm={4} className="col-6">
                                                <div className="p-3 border border-dashed border-end-0">
                                                    <h5 className="mb-1">
                                                        <span className="counter-value" data-target="8.62">
                                                            <CountUp start={0} end={8.62} decimals={2} suffix={"k"} duration={4} />
                                                        </span>
                                                    </h5>
                                                    <p className="text-muted mb-0">Creators</p>
                                                </div>
                                            </Col>
                                        </Row>
                                        <MarketplaceChart series={chartData} dataColors='["--vz-primary","--vz-success", "--vz-gray-300"]' />
                                    </div>
                                </Col>

                                <Col xxl={4}>
                                    <div className="border-start p-4 h-100 d-flex flex-column">

                                        <div className="w-100">
                                            <div className="d-flex align-items-center">
                                                <img src={"https://img.themesbrand.com/velzon/images/img-2.gif"} className="img-fluid avatar-xs rounded-circle object-cover" alt="" />
                                                <div className="ms-3 flex-grow-1">
                                                    <h5 className="fs-16 mb-1">Trendy Fashion Portraits</h5>
                                                    <p className="text-muted mb-0">Artwork</p>
                                                </div>

                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                                        <i className="ri-share-line fs-18"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu className="dropdown-menu-end">
                                                        <DropdownItem href="#"><i className="ri-twitter-fill text-primary align-bottom me-1"></i> Twitter</DropdownItem>
                                                        <DropdownItem href="#"><i className="ri-facebook-circle-fill text-info align-bottom me-1"></i> Facebook</DropdownItem>
                                                        <DropdownItem href="#"><i className="ri-google-fill text-danger align-bottom me-1"></i> Google</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>

                                            </div>

                                            <h3 className="ff-secondary fw-bold mt-4"><i className="mdi mdi-ethereum text-primary"></i> 346.12 ETH</h3>
                                            <p className="text-success mb-3">+586.85 (40.6%)</p>

                                            <p className="text-muted">NFT art is a digital asset that is collectable, unique, and non-transferrable, Cortes explained Every NFT is unique duplicated.</p>

                                            <div className="d-flex align-items-end justify-content-between mt-4">
                                                <div>
                                                    <p className="fs-14 text-muted mb-1">Current Bid</p>
                                                    <h4 className="fs-20 ff-secondary fw-semibold mb-0">342.74 ETH</h4>
                                                </div>

                                                <div>
                                                    <p className="fs-14 text-muted mb-1">Highest Bid</p>
                                                    <h4 className="fs-20 ff-secondary fw-semibold mb-0">346.67 ETH</h4>
                                                </div>
                                            </div>

                                            <div className="dash-countdown mt-4 pt-1">
                                                <Countdown date="2023/1/1" renderer={renderer} />
                                            </div>

                                            <Row className="mt-4 pt-2">
                                                <Col>
                                                    <Link to="/apps-nft-item-details" className="btn btn-primary w-100">View Details</Link>
                                                </Col>
                                                <Col>
                                                    <button className="btn btn-info w-100">Bid Now</button>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                </Col>
                            </Row>
                        </CardBody>
                    </Card>
                </Col>
            </Row>
        </React.Fragment>
    );
};

export default Marketplace;