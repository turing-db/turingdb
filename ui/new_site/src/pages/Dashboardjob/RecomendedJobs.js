import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Col, Row } from 'reactstrap';
import { Grid, _ } from 'gridjs-react';

const RecomendedJobs = () => {
    const data = [
        [
            "Full Stack Engineer",
            "Syntyce Solutions",
            "Zuweihir, UAE",
            "	$650 - $900",
            "	0-1+ year",
            "	Part Time",
        ],
        [
            "UI/UX designer",
            "Zoetic Fashion",
            "Cullera, Spain",
            "	$400+",
            "	$600 - $870",
            "Freelancer",
        ],
        [
            "Project Manager",
            "Meta4Systems",
            "Limestone, US",
            "$210 - $300",
            "0-2+ year",
            "Intership",
        ],
        [
            "Assistant / Store Keeper",
            "Moetic Fashion",
            "Limestone, US",
            "$600 - $870",
            "0-3 year",
            "Full Time",
        ],
        [
            "Marketing Director",
            "Meta4Systems",
            "Vinninga, Sweden",
            "$210 - $300",
            "0-2 year",
            "	Full Time",
        ],
        [
            "Marketing Director",
            "Zoetic Fashion",
            "Quesada, US",
            "	$600 - $870",
            "	0-5 year",
            "Freelancer",
        ],
    ];

    const [jobList, setJobList] = useState(data);

    const searchJob = (ele) => {
        let search = ele.target.value;
        if (search) {
            search = search.toUpperCase();
            setJobList(data.filter((data) => (data[0].toUpperCase().includes(search) || data[1].toUpperCase().includes(search))));
        } else {
            setJobList(data);
        }
    };
    return (
        <React.Fragment>
            <Col lg={12}>
                <Card>
                    <CardHeader>
                        <Row className="g-4 align-items-center">
                            <Col className="col-sm-auto">
                                <div>
                                    <h4 className="card-title mb-0 flex-grow-1">
                                        Recomended Jobs
                                    </h4>
                                </div>
                            </Col>
                            <Col className="col-sm">
                                <div className="d-flex justify-content-sm-end">
                                    <div className="search-box ms-2">
                                        <input
                                            type="text"
                                            className="form-control"
                                            id="searchResultList"
                                            placeholder="Search for jobs..."
                                            onKeyUp={(e) => searchJob(e)}
                                        />
                                        <i className="ri-search-line search-icon"></i>
                                    </div>
                                </div>
                            </Col>
                        </Row>
                    </CardHeader>
                    <CardBody>
                        <Grid
                            data={jobList}
                            columns={[
                                "Position",
                                "Company Name",
                                "Location",
                                "Salary",
                                "Experience",
                                "Job Type",
                            ]}
                            sort={true}
                            pagination={{ enabled: true, limit: 5 }}
                        />
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default RecomendedJobs;