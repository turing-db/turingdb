import React, { useState } from 'react';
import { Card, Col, Collapse, Container, Row } from 'reactstrap';
import classnames from "classnames";

// Import Images
import faqImg from "../../../assets/images/faq-img.png";

const Faqs = () => {
  document.title="FAQs | Velzon - React Admin & Dashboard Template";

  const [col1, setcol1] = useState(true)
  const [col2, setcol2] = useState(false)
  const [col3, setcol3] = useState(false)
  const [col4, setcol4] = useState(false)

  const [col5, setcol5] = useState(false)
  const [col6, setcol6] = useState(true)
  const [col7, setcol7] = useState(false)
  const [col8, setcol8] = useState(false)

  const [col9, setcol9] = useState(true)
  const [col10, setcol10] = useState(false)
  const [col11, setcol11] = useState(false)
  const [col12, setcol12] = useState(false)


  const t_col1 = () => {
    setcol1(!col1)
    setcol2(false)
    setcol3(false)
    setcol4(false)

  }

  const t_col2 = () => {
    setcol2(!col2)
    setcol1(false)
    setcol3(false)
    setcol4(false)

  }

  const t_col3 = () => {
    setcol3(!col3)
    setcol1(false)
    setcol2(false)
    setcol4(false)

  }  
  
  const t_col4 = () => {
    setcol4(!col4)
    setcol1(false)
    setcol2(false)
    setcol3(false)
  }  

  const t_col5 = () => {
    setcol5(!col5)
    setcol6(false)
    setcol7(false)
    setcol8(false)

  }

  const t_col6 = () => {
    setcol6(!col6)
    setcol5(false)
    setcol7(false)
    setcol8(false)

  }

  const t_col7 = () => {
    setcol7(!col7)
    setcol6(false)
    setcol5(false)
    setcol8(false)

  }  
  
  const t_col8 = () => {
    setcol8(!col8)
    setcol5(false)
    setcol6(false)
    setcol7(false)
  }

  const t_col9 = () => {
    setcol9(!col9)
    setcol10(false)
    setcol11(false)
    setcol12(false)

  }

  const t_col10 = () => {
    setcol10(!col10)
    setcol11(false)
    setcol12(false)
    setcol9(false)

  }

  const t_col11 = () => {
    setcol11(!col11)
    setcol9(false)
    setcol10(false)
    setcol12(false)

  }  
  
  const t_col12 = () => {
    setcol12(!col12)
    setcol9(false)
    setcol10(false)
    setcol11(false)
  }
  return (
    <React.Fragment>
       <div className="page-content">                  
                    <Container fluid>
                        <Row>
                            <Col lg={12}>
                                <Card className="rounded-0 bg-soft-success mx-n4 mt-n4 border-top">
                                    <div className="px-4">
                                        <Row>
                                            <Col xxl={5} className="align-self-center">
                                                <div className="py-4">
                                                    <h4 className="display-6 coming-soon-text">Frequently asked questions</h4>
                                                    <p className="text-muted fs-15 mt-3">If you can not find answer to your question in our FAQ, you can always contact us or email us. We will answer you shortly!</p>
                                                    <div className="hstack flex-wrap gap-2">
                                                        <button type="button" className="btn btn-primary btn-label rounded-pill"><i className="ri-mail-line label-icon align-middle rounded-pill fs-16 me-2"></i> Email Us</button>
                                                        <button type="button" className="btn btn-info btn-label rounded-pill"><i className="ri-twitter-line label-icon align-middle rounded-pill fs-16 me-2"></i> Send Us Tweet</button>
                                                    </div>
                                                </div>
                                            </Col>
                                            <div className="col-xxl-3 ms-auto">
                                                <div className="mb-n5 pb-1 faq-img d-none d-xxl-block">
                                                    <img src={faqImg} alt="" className="img-fluid"/>
                                                </div>
                                            </div>
                                        </Row>
                                    </div>                                    
                                </Card>                              
                                <Row className="justify-content-evenly">
                                    <Col lg={4}>
                                        <div className="mt-3">
                                            <div className="d-flex align-items-center mb-2">
                                                <div className="flex-shrink-0 me-1">
                                                    <i className="ri-question-line fs-24 align-middle text-success me-1"></i>
                                                </div>
                                                <div className="flex-grow-1">
                                                    <h5 className="fs-17 mb-0 fw-semibold">General Questions</h5>
                                                </div>
                                            </div>

                                            <div className="accordion accordion-border-box" id="genques-accordion">
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="genques-headingOne">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col1 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col1}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                          What is Lorem Ipsum ?
                                                      </button>
                                                    </h2>
                                                    <Collapse isOpen={col1} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            If several languages coalesce, the grammar of the resulting language is more simple and regular than that of the individual languages. The new common language will be more simple and regular than the existing European languages. It will be as simple their most common words.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="genques-headingTwo">
                                                    <button
                                                      className={classnames(
                                                        "accordion-button",
                                                        "fw-medium",
                                                        { collapsed: !col2 }
                                                      )}
                                                      type="button"
                                                      onClick={t_col2}
                                                      style={{ cursor: "pointer" }}
                                                    >
                                                            Why do we use it ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col2} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            The new common language will be more simple and regular than the existing European languages. It will be as simple as Occidental; in fact, it will be Occidental. To an English person, it will seem like simplified English, as a skeptical Cambridge friend of mine told me what Occidental is.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="genques-headingThree">
                                                    <button
                                                      className={classnames(
                                                        "accordion-button",
                                                        "fw-medium",
                                                        { collapsed: !col3 }
                                                      )}
                                                      type="button"
                                                      onClick={t_col3}
                                                      style={{ cursor: "pointer" }}
                                                    >
                                                            Where does it come from ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col3} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            he wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="genques-headingFour">
                                                    <button
                                                      className={classnames(
                                                        "accordion-button",
                                                        "fw-medium",
                                                        { collapsed: !col4 }
                                                      )}
                                                      type="button"
                                                      onClick={t_col4}
                                                      style={{ cursor: "pointer" }}
                                                    >
                                                            Where can I get some ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col4} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                            </div>
                                        </div>
                                    </Col>

                                   <Col lg={4}>
                                        <div className="mt-3">
                                            <div className="d-flex align-items-center mb-2">
                                                <div className="flex-shrink-0 me-1">
                                                    <i className="ri-user-settings-line fs-24 align-middle text-success me-1"></i>
                                                </div>
                                                <div className="flex-grow-1">
                                                    <h5 className="fs-17 mb-0 fw-semibold">Manage Account</h5>
                                                </div>
                                            </div>

                                            <div className="accordion accordion-border-box" id="manageaccount-accordion">
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="manageaccount-headingOne">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col5 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col5}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            Where can I get some ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col5} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            If several languages coalesce, the grammar of the resulting language is more simple and regular than that of the individual languages. The new common language will be more simple and regular than the existing European languages. It will be as simple their most common words.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="manageaccount-headingTwo">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col6 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col6}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            Where does it come from ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col6} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            The new common language will be more simple and regular than the existing European languages. It will be as simple as Occidental; in fact, it will be Occidental. To an English person, it will seem like simplified English, as a skeptical Cambridge friend of mine told me what Occidental is.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="manageaccount-headingThree">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col7 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col7}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            Why do we use it ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col7} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            he wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="manageaccount-headingFour">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col8 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col8}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            What is Lorem Ipsum ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col8} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                            </div>
                                        </div>
                                    </Col>
                                    
                                    <Col lg={4}>
                                        <div className="mt-3">
                                            <div className="d-flex align-items-center mb-2">
                                                <div className="flex-shrink-0 me-1">
                                                    <i className="ri-shield-keyhole-line fs-24 align-middle text-success me-1"></i>
                                                </div>
                                                <div className="flex-grow-1">
                                                    <h5 className="fs-17 mb-0 fw-semibold">Privacy &amp; Security</h5>
                                                </div>
                                            </div>

                                            <div className="accordion accordion-border-box" id="privacy-accordion">
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="privacy-headingOne">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col9 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col9}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            Why do we use it ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col9} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            If several languages coalesce, the grammar of the resulting language is more simple and regular than that of the individual languages. The new common language will be more simple and regular than the existing European languages. It will be as simple their most common words.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="privacy-headingTwo">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col10 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col10}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            Where can I get some ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col10} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            The new common language will be more simple and regular than the existing European languages. It will be as simple as Occidental; in fact, it will be Occidental. To an English person, it will seem like simplified English, as a skeptical Cambridge friend of mine told me what Occidental is.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="privacy-headingThree">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col11 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col11}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            What is Lorem Ipsum ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col11} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            he wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                                <div className="accordion-item">
                                                    <h2 className="accordion-header" id="privacy-headingFour">
                                                    <button
                                                        className={classnames(
                                                          "accordion-button",
                                                          "fw-medium",
                                                          { collapsed: !col12 }
                                                        )}
                                                        type="button"
                                                        onClick={t_col12}
                                                        style={{ cursor: "pointer" }}
                                                      >
                                                            Where does it come from ?
                                                        </button>
                                                    </h2>
                                                    <Collapse isOpen={col12} className="accordion-collapse">
                                                        <div className="accordion-body">
                                                            Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                        </div>
                                                    </Collapse>
                                                </div>
                                            </div>
                                        </div>
                                    </Col>
                                </Row>
                            </Col>
                        </Row>                        
                    </Container>                    
                </div>
    </React.Fragment>
  )
}

export default Faqs