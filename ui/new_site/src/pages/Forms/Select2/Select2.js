import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import Select from "react-select";

const Select2 = () => {
    document.title = "Select2 | Velzon - React Admin & Dashboard Template";

    const [sortBy, setsortBy] = useState(null);
    const [selectMulti, setselectMulti] = useState(null);
    const [ajaxSelect, setajaxSelect] = useState(null);
    const [temp, settemp] = useState(null);
    const [selTemp, setselTemp] = useState(null);
    const [dissortBy, setdissortBy] = useState(null);
    const [disselectMulti, setdisselectMulti] = useState(null);

    const [disable, setdisable] = useState(false);


    const sortbyname = [
        {
            options: [
                { label: "Alabama", value: "AL" },
                { label: "Madrid", value: "MA" },
                { label: "Toronto", value: "TO" },
                { label: "Londan", value: "LO" },
                { label: "Wyoming", value: "WY" }
            ],
        },
    ];

    const sortbyMulti = [
        {
            label: "UK",
            options: [
                { label: "London", value: "London" },
                { label: "Manchester", value: "Manchester" },
                { label: "Liverpool", value: "Liverpool" }
            ]
        },
        {
            label: "FR",
            options: [
                { label: "Paris", value: "Paris" },
                { label: "Lyon", value: "Lyon" },
                { label: "Marseille", value: "Marseille" }
            ]
        },
        {
            label: "SP",
            options: [
                { label: "Madrid", value: "Madrid" },
                { label: "Barcelona", value: "Barcelona" },
                { label: "Malaga", value: "Malaga" }
            ]
        },
        {
            label: "CA",
            options: [
                { label: "Montreal", value: "Montreal" },
                { label: "Toronto", value: "Toronto" },
                { label: "Vancouver", value: "Vancouver" }
            ]
        }

    ];

    const ajaxSelectOption = [
        {
            options: [
                { label: "enhancement", value: "enhancement" },
                { label: "bug", value: "bug" },
                { label: "duplicate", value: "duplicate" },
                { label: "invalid", value: "invalid" },
                { label: "wontfix", value: "wontfix" }
            ],
        },
    ];

    const tempOption = [
        {
            label: "Alaskan/Hawaiian Time Zone",
            options: [
                { label: "Alaska", value: "AK" },
                { label: "Hawaii", value: "HI" },
            ]
        },
        {
            label: "Pacific Time Zone",
            options: [
                { label: "California", value: "CA" },
                { label: "Nevada", value: "NV" },
                { label: "Oregon", value: "OR" },
                { label: "Washington", value: "WA" }
            ]
        },
    ];

    const selTempOption = [
        {
            label: "Alaskan/Hawaiian Time Zone",
            options: [
                { label: "Alaska", value: "AK" },
                { label: "Hawaii", value: "HI" },
            ]
        },
        {
            label: "Pacific Time Zone",
            options: [
                { label: "California", value: "CA" },
                { label: "Nevada", value: "NV" },
                { label: "Oregon", value: "OR" },
                { label: "Washington", value: "WA" }
            ]
        },
    ];

    const dissortbyname = [
        {
            options: [
                { label: "Alabama", value: "AL" },
                { label: "Madrid", value: "MA" },
                { label: "Toronto", value: "TO" },
                { label: "Londan", value: "LO" },
                { label: "Wyoming", value: "WY" }
            ],
        },
    ];

    const dissortbyMulti = [
        {
            label: "UK",
            options: [
                { label: "London", value: "London" },
                { label: "Manchester", value: "Manchester" },
                { label: "Liverpool", value: "Liverpool" }
            ]
        },
        {
            label: "FR",
            options: [
                { label: "Paris", value: "Paris" },
                { label: "Lyon", value: "Lyon" },
                { label: "Marseille", value: "Marseille" }
            ]
        },
        {
            label: "SP",
            options: [
                { label: "Madrid", value: "Madrid" },
                { label: "Barcelona", value: "Barcelona" },
                { label: "Malaga", value: "Malaga" }
            ]
        },
        {
            label: "CA",
            options: [
                { label: "Montreal", value: "Montreal" },
                { label: "Toronto", value: "Toronto" },
                { label: "Vancouver", value: "Vancouver" }
            ]
        }

    ];

    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Select2" pageTitle="Forms" />
                    <div className="alert alert-danger" role="alert">
                        This is <strong>Select2</strong> page in wihch we have used <b>jQuery</b> with cdn link!
                    </div>
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h5 className="card-title mb-0">Basic Select2</h5>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">Use <code>js-example-basic-single</code>, <code>js-example-basic-multiple</code>, <code>js-example-data-array</code>, <code>js-example-templating</code>, <code>select-flag-templating</code>,  class to show select2 example.</p>
                                    <Row className="g-4">
                                        <Col lg={4}>
                                            <h6 className="fw-semibold">Basic Select</h6>
                                            <Select
                                                value={sortBy}
                                                onChange={(sortBy) => {
                                                    setsortBy(sortBy);
                                                }}
                                                options={sortbyname}
                                                id="choices-single-default"
                                                className="js-example-basic-single mb-0"
                                                name="state"
                                            />
                                        </Col>
                                        <Col lg={4}>
                                            <h6 className="fw-semibold">Multi Select</h6>

                                            <Select
                                                value={selectMulti}
                                                isMulti={true}
                                                onChange={(sortBy) => {
                                                    setselectMulti(sortBy);
                                                }}
                                                options={sortbyMulti}
                                                classNamePrefix="js-example-basic-multiple mb-0"
                                            />
                                        </Col>
                                        <Col lg={4}>
                                            <h6 className="fw-semibold">Ajax Select</h6>

                                            <Select
                                                value={ajaxSelect}
                                                onChange={(sortBy) => {
                                                    setajaxSelect(sortBy);
                                                }}
                                                options={ajaxSelectOption}
                                                classNamePrefix="js-example-data-array"
                                                isLoading={true}
                                            />
                                        </Col>

                                        <Col lg={4}>
                                            <h6 className="fw-semibold">Templating</h6>

                                            <Select
                                                value={temp}
                                                onChange={(sortBy) => {
                                                    settemp(sortBy);
                                                }}
                                                options={tempOption}
                                                id="choices-single-default"
                                                className="js-example-templating mb-0"
                                            />
                                        </Col>
                                        <Col lg={4}>
                                            <h6 className="fw-semibold">Selections Templating</h6>

                                            <Select
                                                value={selTemp}
                                                onChange={(sortBy) => {
                                                    setselTemp(sortBy);
                                                }}
                                                options={selTempOption}
                                                id="choices-single-default"
                                                className="select-flag-templating mb-0"
                                            />
                                        </Col>
                                    </Row>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h5 className="card-title mb-0">Disabling a Select2 Control</h5>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">Select2 will respond to the disabled attribute on <code>&lt;select&gt;</code> elements. You can also initialize Select2 with disabled: true to get the same effect.</p>
                                    <div className="vstack gap-3">

                                        <Select
                                            value={dissortBy}
                                            onChange={(sortBy) => {
                                                setdissortBy(sortBy);
                                            }}
                                            options={dissortbyname}
                                            className="js-example-disabled mb-0"
                                            isDisabled={disable}
                                        />

                                        <Select
                                            value={disselectMulti}
                                            isMulti={true}
                                            onChange={(sortBy) => {
                                                setdisselectMulti(sortBy);
                                            }}
                                            options={dissortbyMulti}
                                            classNamePrefix="js-example-disabled-multi mb-0"
                                            isDisabled={disable}
                                        />
                                    </div>
                                    <div className="hstack gap-2 mt-3">
                                        <button type="button" className="js-programmatic-enable btn btn-primary" onClick={() => setdisable(false)}>
                                            Enable
                                        </button>
                                        <button type="button" className="js-programmatic-disable btn btn-success" onClick={() => setdisable(true)}>
                                            Disable
                                        </button>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default Select2;