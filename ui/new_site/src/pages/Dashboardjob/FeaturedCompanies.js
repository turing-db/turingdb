import React from "react";
import { Link } from "react-router-dom";
import { Card, CardBody, Col, Table } from "reactstrap";
import { featuredCompany } from "../../common/data/dashboardjobs";

const FeaturedCompanies = () => {
    return (
        <React.Fragment>
            <Col className="col-xl-6">
                <Card className="card card-height-100">
                    <div className="card-header align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Featured Companies</h4>
                        <div className="flex-shrink-0">
                            <Link to="#!" className="btn btn-soft-primary btn-sm">
                                View All Companies{" "}
                                <i className="ri-arrow-right-line align-bottom"></i>
                            </Link>
                        </div>
                    </div>

                    <CardBody className="card-body">
                        <div className="table-responsive table-card">
                            <Table className="table table-centered table-hover align-middle table-nowrap mb-0">
                                <tbody>
                                    {featuredCompany.map((company, key) => (
                                        <tr key={key}>
                                            <td>
                                                <div className="d-flex align-items-center">
                                                    <div className="avatar-xs me-2 flex-shrink-0">
                                                        <div className="avatar-title bg-soft-secondary rounded">
                                                            <img src={company.img} alt="" height="16" />
                                                        </div>
                                                    </div>
                                                    <h6 className="mb-0">{company.lable}</h6>
                                                </div>
                                            </td>
                                            <td>
                                                <i className="ri-map-pin-2-line text-primary me-1 align-bottom"></i>{" "}
                                                {company.city}
                                            </td>
                                            <td>
                                                <ul className="list-inline mb-0">
                                                    <li className="list-inline-item">
                                                        <Link to="#!" className="link-secondary">
                                                            <i className="ri-facebook-fill"></i>
                                                        </Link>
                                                    </li>
                                                    <li className="list-inline-item">
                                                        <Link to="#!" className="link-danger">
                                                            <i className="ri-mail-line"></i>
                                                        </Link>
                                                    </li>
                                                    <li className="list-inline-item">
                                                        <Link to="#!" className="link-primary">
                                                            <i className="ri-global-line"></i>
                                                        </Link>
                                                    </li>
                                                    <li className="list-inline-item">
                                                        <Link to="#!" className="link-info">
                                                            <i className="ri-twitter-line"></i>
                                                        </Link>
                                                    </li>
                                                </ul>
                                            </td>
                                            <td>
                                                   <Link to="#!" className="btn btn-link btn-sm">
                                                    View More{" "}
                                                    <i className="ri-arrow-right-line align-bottom"></i>
                                                </Link>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        </div>
                        <div className="align-items-center mt-4 pt-2 justify-content-between d-flex">
                            <div className="flex-shrink-0">
                                <div className="text-muted">
                                    Showing <span className="fw-semibold">5</span> of{" "}
                                    <span className="fw-semibold">25</span> Results
                                </div>
                            </div>
                            <ul className="pagination pagination-separated pagination-sm mb-0">
                                <li className="page-item disabled">
                                    <Link to="#" className="page-link">
                                        ←
                                    </Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">
                                        1
                                    </Link>
                                </li>
                                <li className="page-item active">
                                    <Link to="#" className="page-link">
                                        2
                                    </Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">
                                        3
                                    </Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">
                                        →
                                    </Link>
                                </li>
                            </ul>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default FeaturedCompanies;
