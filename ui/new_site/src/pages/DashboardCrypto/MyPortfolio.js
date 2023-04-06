import React, { useState, useEffect } from 'react';
import { DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { useSelector, useDispatch } from "react-redux";
import { getPortfolioChartsData } from '../../slices/thunks';

//import Images
import btc from "../../assets/images/svg/crypto-icons/btc.svg";
import eth from "../../assets/images/svg/crypto-icons/eth.svg";
import ltc from "../../assets/images/svg/crypto-icons/ltc.svg";
import dash from "../../assets/images/svg/crypto-icons/dash.svg";

import { PortfolioCharts } from './DashboardCryptoCharts';

const MyPortfolio = () => {
    const dispatch = useDispatch();

    const [chartData, setchartData] = useState([]);

    const { portfolioData } = useSelector(state => ({
        portfolioData: state.DashboardCrypto.portfolioData
    }));

    useEffect(() => {
        setchartData(portfolioData);
    }, [portfolioData]);

    const [seletedMonth, setSeletedMonth] = useState("Btc");
    const onChangeChartPeriod = pType => {
        setSeletedMonth(pType);
        dispatch(getPortfolioChartsData(pType));
    };

    useEffect(() => {
        dispatch(getPortfolioChartsData("btc"));
    }, [dispatch]);
    return (
        <React.Fragment>
            <div className="col-xxl-3">
                <div className="card card-height-100">
                    <div className="card-header border-0 align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">My Portfolio</h4>
                        <div>
                            <UncontrolledDropdown direction='start'>
                                <DropdownToggle tag="button" className="btn btn-soft-primary btn-sm" >
                                    <span className="text-uppercase">{seletedMonth}<i className="mdi mdi-chevron-down align-middle ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu dropdown-menu-end">
                                    <DropdownItem onClick={() => { onChangeChartPeriod("btc"); }} className={seletedMonth === "Btc" ? "active" : ""}>BTC</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("usd"); }} className={seletedMonth === "usd" ? "active" : ""}>USD</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("euro"); }} className={seletedMonth === "euro" ? "active" : ""}>Euro</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </div>
                    <div className="card-body">
                        <div id="portfolio_donut_charts"  className="apex-charts" dir="ltr">
                            <PortfolioCharts series={chartData} dataColors='["--vz-primary", "--vz-info", "--vz-warning", "--vz-success"]'/>
                        </div>

                        <ul className="list-group list-group-flush border-dashed mb-0 mt-3 pt-2">
                            <li className="list-group-item px-0">
                                <div className="d-flex">
                                    <div className="flex-shrink-0 avatar-xs">
                                        <span className="avatar-title bg-light p-1 rounded-circle">
                                            <img src={btc} className="img-fluid" alt="" />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-2">
                                        <h6 className="mb-1">Bitcoin</h6>
                                        <p className="fs-13 mb-0 text-muted"><i className="mdi mdi-circle fs-10 align-middle text-primary me-1"></i>BTC</p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1">BTC 0.00584875</h6>
                                        <p className="text-success fs-13 mb-0">${chartData[0]}.12</p>
                                    </div>
                                </div>
                            </li>
                            <li className="list-group-item px-0">
                                <div className="d-flex">
                                    <div className="flex-shrink-0 avatar-xs">
                                        <span className="avatar-title bg-light p-1 rounded-circle">
                                            <img src={eth} className="img-fluid" alt="" />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-2">
                                        <h6 className="mb-1">Ethereum</h6>
                                        <p className="fs-13 mb-0 text-muted"><i className="mdi mdi-circle fs-10 align-middle text-info me-1"></i>ETH</p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1">ETH 2.25842108</h6>
                                        <p className="text-danger mb-0 fs-13">${chartData[1]}.18</p>
                                    </div>
                                </div>
                            </li>
                            <li className="list-group-item px-0">
                                <div className="d-flex">
                                    <div className="flex-shrink-0 avatar-xs">
                                        <span className="avatar-title bg-light p-1 rounded-circle">
                                            <img src={ltc} className="img-fluid" alt="" />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-2">
                                        <h6 className="mb-1">Litecoin</h6>
                                        <p className="fs-13 mb-0 text-muted"><i className="mdi mdi-circle fs-10 align-middle text-warning me-1"></i>LTC</p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1">LTC 10.58963217</h6>
                                        <p className="text-success mb-0 fs-13">${chartData[2]}.58</p>
                                    </div>
                                </div>
                            </li>
                            <li className="list-group-item px-0 pb-0">
                                <div className="d-flex">
                                    <div className="flex-shrink-0 avatar-xs">
                                        <span className="avatar-title bg-light p-1 rounded-circle">
                                            <img src={dash} className="img-fluid" alt="" />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-2">
                                        <h6 className="mb-1">Dash</h6>
                                        <p className="fs-13 mb-0 text-muted"><i className="mdi mdi-circle fs-10 align-middle text-success me-1"></i>DASH</p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1">DASH 204.28565885</h6>
                                        <p className="text-success mb-0 fs-13">${chartData[3]}.84</p>
                                    </div>
                                </div>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </React.Fragment>
    );
};

export default MyPortfolio;