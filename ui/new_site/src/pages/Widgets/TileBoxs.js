import React from 'react';
import { Card, CardBody, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from 'reactstrap';
import { Link } from 'react-router-dom';

import CountUp from "react-countup";

//Import Icons
import FeatherIcon from "feather-icons-react";

import { tileBoxs1, tileBoxs2, tileBoxs3, tileBoxes4, tileBoxes5 } from "../../common/data/index";

const TileBoxs = () => {
    return (
        <React.Fragment>
            <Row>
                <Col className="col-12">
                    <h5 className="text-decoration-underline mb-3 pb-1">Tile Boxs</h5>
                </Col>
            </Row>

            <Row>
                {(tileBoxs1 || []).map((item, key) => (
                    <Col xl={3} md={6} key={key}>
                        <Card className={"card-animate " + item.bgColor}>
                            <CardBody>
                                <div className="d-flex align-items-center">
                                    <div className="flex-grow-1">
                                        <p className={"text-uppercase fw-medium mb-0 text-" + item.labelClass}>{item.label}</p>
                                    </div>
                                    <div className="flex-shrink-0">
                                        <h5 className={"fs-14 mb-0 text-" + item.percentageClass}>
                                            <i className={"fs-13 align-middle " + item.percentageIcon}></i> {item.percentage}
                                        </h5>
                                    </div>
                                </div>
                                <div className="d-flex align-items-end justify-content-between mt-4">
                                    <div>
                                        <h4 className={"fs-22 fw-semibold ff-secondary mb-4 " + item.counterClass}><span className="counter-value" data-target="559.25">
                                            <CountUp
                                                start={0}
                                                prefix={item.prefix}
                                                suffix={item.suffix}
                                                separator={item.separator}
                                                end={item.counter}
                                                decimals={item.decimals}
                                                duration={4}
                                            />
                                        </span></h4>
                                        <Link to="#" className={"text-decoration-underline " + item.captionClass}>{item.caption}</Link>
                                    </div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className={"avatar-title rounded fs-3 bg-soft-" + item.iconClass}>
                                            <i className={item.icon + " text-" + item.iconClass}></i>
                                        </span>
                                    </div>
                                </div>
                            </CardBody>
                        </Card>
                    </Col>))}
            </Row>

            <Row>
                <Col xl={12}>
                    <Card className="crm-widget">
                        <CardBody className="p-0">
                            <Row className="row-cols-md-3 row-cols-1">

                                {(tileBoxs2 || []).map((item, key) => (
                                    <Col className={item.id === 4 ? "col-lg" : "col-lg border-end"} key={key}>
                                        <div className="mt-3 mt-md-0 py-4 px-3">
                                            <h5 className="text-muted text-uppercase fs-13">{item.label} <i className={"fs-18 float-end align-middle " + item.badge}></i></h5>
                                            <div className="d-flex align-items-center">
                                                <div className="flex-shrink-0">
                                                    <i className={"display-6 text-muted " + item.icon}></i>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h2 className="mb-0"><span className="counter-value">
                                                        <CountUp
                                                            start={0}
                                                            prefix={item.prefix}
                                                            suffix={item.suffix}
                                                            separator={item.separator}
                                                            end={item.counter}
                                                            decimals={item.decimals}
                                                            duration={4}
                                                        /></span></h2>
                                                </div>
                                            </div>
                                        </div>
                                    </Col>))}
                            </Row>
                        </CardBody>
                    </Card>
                </Col>
            </Row>

            <Row>
                {(tileBoxs3 || []).map((item, key) => (<Col xl={3} md={6} key={key}>
                    <Card className={"card-animate " + item.bgColor}>
                        <CardBody>
                            <div className="d-flex justify-content-between">
                                <div>
                                    <p className={"fw-medium mb-0 text-" + item.labelClass}>{item.label}</p>
                                    <h2 className={"mt-4 ff-secondary fw-semibold " + item.counterClass}>
                                        {item.subCounter.map((item, key) => (<span className="counter-value" key={key}>
                                            <CountUp
                                                start={0}
                                                prefix={item.prefix}
                                                suffix={item.suffix}
                                                separator={item.separator}
                                                end={item.counter}
                                                decimals={item.decimals}
                                                duration={4}
                                            />
                                        </span>))}
                                    </h2>
                                    <p className={"mb-0 text-" + item.labelClass}><span className={"mb-0 badge " + item.badgeClass}>
                                        <i className={"align-middle " + item.badge}></i> {item.percentage}
                                    </span> vs. previous month</p>
                                </div>
                                <div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className={"avatar-title rounded-circle fs-2 bg-soft-" + item.iconClass}>
                                            <FeatherIcon
                                                icon={item.feaIcon}
                                                className={"text-" + item.iconClass}
                                            />
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>))}
            </Row>

            <Row>
                {(tileBoxes4 || []).map((item, key) => (<Col xl={3} md={6} key={key}>
                    <Card className={"card-height-100 " + item.bgColor}>
                        <CardBody>
                            <div className="d-flex align-items-center">
                                <div className="avatar-sm flex-shrink-0">
                                    <span className={"avatar-title rounded-2 fs-2 bg-soft-" + item.iconClass + " text-" + item.iconClass}>
                                        <i className={item.icon}></i>
                                    </span>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <p className={"text-uppercase fw-medium mb-3 text-" + item.labelClass}>{item.label}</p>
                                    <h4 className={"fs-4 mb-3 " + item.counterClass}><span className="counter-value">
                                        <CountUp
                                            start={0}
                                            prefix={item.prefix}
                                            suffix={item.suffix}
                                            separator={item.separator}
                                            end={item.counter}
                                            decimals={item.decimals}
                                            duration={4}
                                        />
                                    </span></h4>
                                    <p className={"mb-0 text-" + item.captionClass}>{item.caption}</p>
                                </div>
                                <div className="flex-shrink-0 align-self-center">
                                    <span className={"fs-12 badge badge-soft-" + item.percentageClass}><i className={"fs-13 align-middle me-1 " + item.badge}></i>{item.percentage}<span>
                                    </span></span></div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>))}
            </Row>

            <Row>
                {(tileBoxes5 || []).map((item, key) => (
                    <Col xl={3} ms={6} key={key}>
                        <Card className={"card-height-100 " + item.class}>
                            <CardBody>
                                <UncontrolledDropdown className="float-end">
                                    <DropdownToggle tag="a" className="text-reset dropdown-btn" href="#">
                                        <span className="text-muted fs-18"><i className="mdi mdi-dots-vertical"></i></span>
                                    </DropdownToggle>
                                    <DropdownMenu className="dropdown-menu-end">
                                        <DropdownItem>Favorite</DropdownItem>
                                        <DropdownItem>Apply Now</DropdownItem>
                                    </DropdownMenu>
                                </UncontrolledDropdown>
                                <div className="mb-4 pb-2">
                                    <img src={item.img} alt="" className="avatar-sm" />
                                </div>
                                <Link to="#!">
                                    <h6 className="fs-15 fw-semibold">{item.title} <span className="text-muted fs-13">{item.year}</span></h6>
                                </Link>
                                <p className="text-muted mb-0"><i className="ri-building-line align-bottom"></i> Themesbrand <span className="ms-2"><i className="ri-map-pin-2-line align-bottom"></i> California</span></p>
                            </CardBody>
                        </Card>
                    </Col>))}
            </Row>
        </React.Fragment>
    );
};

export default TileBoxs;