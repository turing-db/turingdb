import React, { useState, useEffect } from 'react';
import { Card, CardBody, CardHeader, Col, Row } from 'reactstrap';
import CountUp from "react-countup";
import { useSelector, useDispatch } from "react-redux";
import { AudiencesCharts } from './DashboardAnalyticsCharts';
import { getAudiencesMetricsChartsData } from "../../slices/thunks";

const AudiencesMetrics = () => {
    const dispatch = useDispatch();

    const [chartData, setchartData] = useState([]);

    const { audiencesMetricsData } = useSelector(state => ({
        audiencesMetricsData: state.DashboardAnalytics.audiencesMetricsData
    }));

    useEffect(() => {
        setchartData(audiencesMetricsData);
    }, [audiencesMetricsData]);

    const onChangeChartPeriod = pType => {
        dispatch(getAudiencesMetricsChartsData(pType));
    };

    useEffect(() => {
        dispatch(getAudiencesMetricsChartsData("all"));
    }, [dispatch]);
    return (
        <React.Fragment>
            <Col xl={6}>
                <Card>
                    <CardHeader className="border-0 align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Audiences Metrics</h4>
                        <div className="d-flex gap-1">
                            <button type="button" className="btn btn-soft-secondary btn-sm" onClick={() => { onChangeChartPeriod("all"); }}>
                                ALL
                            </button>
                            <button type="button" className="btn btn-soft-secondary btn-sm" onClick={() => { onChangeChartPeriod("monthly"); }}>
                                1M
                            </button>
                            <button type="button" className="btn btn-soft-secondary btn-sm" onClick={() => { onChangeChartPeriod("halfyearly"); }}>
                                6M
                            </button>
                            <button type="button" className="btn btn-soft-primary btn-sm" onClick={() => { onChangeChartPeriod("yearly"); }}>
                                1Y
                            </button>
                        </div>
                    </CardHeader>
                    <CardHeader className="p-0 border-0 bg-soft-light">
                        <Row className="g-0 text-center">
                            <Col xs={6} sm={4}>
                                <div className="p-3 border border-dashed border-start-0">
                                    <h5 className="mb-1"><span className="counter-value" data-target="854">
                                        <CountUp
                                            start={0}
                                            end={854}
                                            duration={3}
                                        />
                                    </span>
                                        <span className="text-success ms-1 fs-13">49%<i className="ri-arrow-right-up-line ms-1 align-middle"></i></span>
                                    </h5>
                                    <p className="text-muted mb-0">Avg. Session</p>
                                </div>
                            </Col>
                            <Col xs={6} sm={4}>
                                <div className="p-3 border border-dashed border-start-0">
                                    <h5 className="mb-1"><span className="counter-value" data-target="1278">
                                        <CountUp
                                            start={0}
                                            end={1278}
                                            duration={3}
                                            separator=","
                                        />
                                    </span>
                                        <span className="text-success ms-1 fs-13">60%<i className="ri-arrow-right-up-line ms-1 align-middle"></i></span>
                                    </h5>
                                    <p className="text-muted mb-0">Conversion Rate</p>
                                </div>
                            </Col>
                            <Col xs={6} sm={4}>
                                <div className="p-3 border border-dashed border-start-0 border-end-0">
                                    <h5 className="mb-1"><span className="counter-value" data-target="3">
                                        <CountUp
                                            start={0}
                                            end={3}
                                            duration={3}
                                        />
                                    </span>m <span className="counter-value" data-target="40">
                                            <CountUp
                                                start={0}
                                                end={40}
                                                duration={3}
                                            />
                                        </span>sec
                                        <span className="text-success ms-1 fs-13">37%<i className="ri-arrow-right-up-line ms-1 align-middle"></i></span>
                                    </h5>
                                    <p className="text-muted mb-0">Avg. Session Duration</p>
                                </div>
                            </Col>
                        </Row>
                    </CardHeader>
                    <CardBody className="p-0 pb-2">
                        <div>
                            <div id="audiences_metrics_charts" className="apex-charts" dir="ltr">
                                <AudiencesCharts series={chartData} dataColors='["--vz-primary", "--vz-gray-300"]' />
                            </div>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default AudiencesMetrics;