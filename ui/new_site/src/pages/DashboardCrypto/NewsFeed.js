import React from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, CardHeader, Col } from 'reactstrap';
import { newsFeed } from '../../common/data';

const NewsFeed = () => {
    return (
        <React.Fragment>
            <Col xxl={4}>
                <Card className='card-height-100'>
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">News Feed</h4>
                        <div>
                            <button type="button" className="btn btn-soft-primary btn-sm">
                                View all
                            </button>
                        </div>
                    </CardHeader>

                    <CardBody>
                        {(newsFeed || []).map((item, key) => (
                            <div className={item.id === 1 ? "d-flex align-middle" : "d-flex mt-4"} key={key}>
                                <div className="flex-shrink-0">
                                    <img src={item.img} className="rounded img-fluid" style={{ height: "60px" }} alt="" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1 lh-base"><Link to="#" className="text-reset">{item.caption}</Link></h6>
                                    <p className="text-muted fs-13 mb-0">{item.date} <i className="mdi mdi-circle-medium align-middle mx-1"></i>{item.time}</p>
                                </div>
                            </div>
                        ))}
                        <div className="mt-3 text-center">
                            <Link to="#" className="text-muted text-decoration-underline">View all News</Link>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default NewsFeed;