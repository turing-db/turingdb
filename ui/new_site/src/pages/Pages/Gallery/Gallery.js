import React, { useState } from 'react';
import { Card, CardBody, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import { Link } from 'react-router-dom';
import Masonry from "react-masonry-component";

import { gallery } from '../../../common/data';

const Gallery = () => {
  document.title = "Gallery | Velzon - React Admin & Dashboard Template";

  const [displayCategory, setCategory] = useState("All");

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Gallery" pageTitle="Pages" />
          <Row>
            <Col lg={12}>
              <div className="">
                <CardBody>
                  <Row>
                    <Col lg={12}>
                      <div className="text-center">
                        <ul className="list-inline categories-filter animation-nav" id="filter">
                          <li className="list-inline-item">
                            <Link to="#" onClick={() => setCategory("All")} className={displayCategory === "All" ? "categories active" : "categories"} data-filter="*">All</Link></li>
                          <li className="list-inline-item"><Link to="#" onClick={() => setCategory("Project")} className={displayCategory === "Project" ? "categories active" : "categories"} data-filter=".project">Project</Link></li>
                          <li className="list-inline-item"><Link to="#" onClick={() => setCategory("Designing")} className={displayCategory === "Designing" ? "categories active" : "categories"} data-filter=".designing">Designing</Link></li>
                          <li className="list-inline-item"><Link to="#" onClick={() => setCategory("Photography")} className={displayCategory === "Photography" ? "categories active" : "categories"} data-filter=".photography">Photography</Link></li>
                          <li className="list-inline-item"><Link to="#" onClick={() => setCategory("Development")} className={displayCategory === "Development" ? "categories active" : "categories"} data-filter=".development">Development</Link></li>
                        </ul>
                      </div>

                      <Masonry className="row gallery-wrapper">
                        {(gallery.filter(({ category }) => displayCategory === category || displayCategory === "All")).map(({ img, title, auther, likes, comments }, key) => (
                          <Col xxl={3} xl={4} sm={6} className="element-item project designing development" key={key}>
                            <Card className="gallery-box">
                              <div className="gallery-container">
                                <Link className="image-popup" to={img} title="">
                                  <img className="gallery-img img-fluid mx-auto" src={img} alt="" />
                                  <div className="gallery-overlay">
                                    <h5 className="overlay-caption">{title}</h5>
                                  </div>
                                </Link>
                              </div>

                              <div className="box-content">
                                <div className="d-flex align-items-center mt-1">
                                  <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">{auther}</Link></div>
                                  <div className="flex-shrink-0">
                                    <div className="d-flex gap-3">
                                      <button type="button" className="btn btn-sm fs-13 btn-link text-body text-decoration-none px-0">
                                        <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> {likes}
                                      </button>
                                      <button type="button" className="btn btn-sm fs-13 btn-link text-body text-decoration-none px-0">
                                        <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> {comments}
                                      </button>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </Card>
                          </Col>
                        ))}

                      </Masonry>
                      <div className="text-center my-2">
                        <Link to="#" className="text-success"><i className="mdi mdi-loading mdi-spin fs-20 align-middle me-2"></i> Load More </Link>
                      </div>
                    </Col>
                  </Row>
                </CardBody>
              </div>
            </Col>
          </Row>


        </Container>

      </div>
    </React.Fragment>
  );
};

export default Gallery;