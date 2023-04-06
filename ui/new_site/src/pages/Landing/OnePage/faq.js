import React, { useState } from 'react';
import { Col, Container, Row, Collapse } from 'reactstrap';
import classnames from "classnames";

const Faqs = () => {

    const [col1, setcol1] = useState(true);
    const [col2, setcol2] = useState(false);
    const [col3, setcol3] = useState(false);
    const [col4, setcol4] = useState(false);

    const [col9, setcol5] = useState(false);
    const [col10, setcol6] = useState(true);
    const [col11, setcol7] = useState(false);
    const [col12, setcol8] = useState(false);

    const t_col1 = () => {
        setcol1(!col1);
        setcol2(false);
        setcol3(false);
        setcol4(false);

    };

    const t_col2 = () => {
        setcol2(!col2);
        setcol1(false);
        setcol3(false);
        setcol4(false);

    };

    const t_col3 = () => {
        setcol3(!col3);
        setcol1(false);
        setcol2(false);
        setcol4(false);

    };

    const t_col4 = () => {
        setcol4(!col4);
        setcol1(false);
        setcol2(false);
        setcol3(false);
    };

    const t_col5 = () => {
        setcol5(!col9);
        setcol6(false);
        setcol7(false);
        setcol8(false);

    };

    const t_col6 = () => {
        setcol6(!col10);
        setcol7(false);
        setcol8(false);
        setcol5(false);

    };

    const t_col7 = () => {
        setcol7(!col11);
        setcol5(false);
        setcol6(false);
        setcol8(false);

    };

    const t_col8 = () => {
        setcol8(!col12);
        setcol5(false);
        setcol6(false);
        setcol7(false);
    };

    return (
        <React.Fragment>
            <section className="section">
                <Container>
                    <Row className="justify-content-center">
                        <Col lg={8}>
                            <div className="text-center mb-5">
                                <h3 className="mb-3 fw-semibold">Frequently Asked Questions</h3>
                                <p className="text-muted mb-4 ff-secondary">If you can not find answer to your question in our FAQ, you can
                                    always contact us or email us. We will answer you shortly!</p>

                                <div className="">
                                    <button type="button" className="btn btn-primary btn-label rounded-pill me-1"><i className="ri-mail-line label-icon align-middle rounded-pill fs-16 me-2"></i> Email Us</button>
                                    <button type="button" className="btn btn-info btn-label rounded-pill"><i className="ri-twitter-line label-icon align-middle rounded-pill fs-16 me-2"></i> Send Us Tweet</button>
                                </div>
                            </div>
                        </Col>
                    </Row>

                    <Row className="g-lg-5 g-4">
                        <Col lg={6}>
                            <div className="d-flex align-items-center mb-2">
                                <div className="flex-shrink-0 me-1">
                                    <i className="ri-question-line fs-24 align-middle text-success me-1"></i>
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="mb-0 fw-semibold">General Questions</h5>
                                </div>
                            </div>
                            <div className="accordion custom-accordionwithicon custom-accordion-border accordion-border-box"
                                id="genques-accordion">
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
                                            What is the purpose of using themes ?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col1} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            A theme is a set of colors, fonts, effects, and more that can be applied to your entire presentation to give it a
                                            consistent, professional look. You've already been using a theme, even if you didn't know it: the default Office theme,
                                            which consists.
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
                                            Can a theme have more than one theme?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col2} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            A story can have as many themes as the reader can identify based on recurring patterns and parallels within the story
                                            itself. In looking at ways to separate themes into a hierarchy, we might find it useful to follow the example of a
                                            single book.
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
                                            What are theme features?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col3} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            Theme features is a set of specific functionality that may be enabled by theme authors. Themes must register each
                                            individual Theme Feature that the author wishes to support. Theme support functions should be called in the theme's
                                            functions.
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
                                            What is simple theme?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col4} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            Simple is a free WordPress theme, by Themify, built exactly what it is named for: simplicity. Immediately upgrade the
                                            quality of your WordPress site with the simple theme To use the built-in Chrome theme editor.
                                        </div>
                                    </Collapse>
                                </div>
                            </div>
                        </Col>

                        <Col lg={6}>
                            <div className="d-flex align-items-center mb-2">
                                <div className="flex-shrink-0 me-1">
                                    <i className="ri-shield-keyhole-line fs-24 align-middle text-success me-1"></i>
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="mb-0 fw-semibold">Privacy &amp; Security</h5>
                                </div>
                            </div>

                            <div className="accordion custom-accordionwithicon custom-accordion-border accordion-border-box"
                                id="privacy-accordion">
                                <div className="accordion-item">
                                    <h2 className="accordion-header" id="privacy-headingOne">
                                        <button
                                            className={classnames(
                                                "accordion-button",
                                                "fw-medium",
                                                { collapsed: !col9 }
                                            )}
                                            type="button"
                                            onClick={t_col5}
                                            style={{ cursor: "pointer" }}
                                        >
                                            Does Word have night mode?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col9} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            You can run Microsoft Word in dark mode, which uses a dark color palette to help reduce eye strain in low light
                                            settings. You can choose to make the document white or black using the Switch Modes button in the ribbon's View tab.
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
                                            onClick={t_col6}
                                            style={{ cursor: "pointer" }}
                                        >
                                            Is theme an opinion?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col10} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            A theme is an opinion the author expresses on the subject, for instance, the author's dissatisfaction with the narrow
                                            confines of French bourgeois marriage during that period theme is an idea that a writer repeats.
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
                                            onClick={t_col7}
                                            style={{ cursor: "pointer" }}
                                        >
                                            How do you develop a theme?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col11} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            A short story, novella, or novel presents a narrative to its reader. Perhaps that narrative involves mystery, terror,
                                            romance, comedy, or all of the above. These works of fiction may also contain memorable characters, vivid
                                            world-building, literary devices.
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
                                            onClick={t_col8}
                                            style={{ cursor: "pointer" }}
                                        >
                                            Do stories need themes?
                                        </button>
                                    </h2>
                                    <Collapse isOpen={col12} className="accordion-collapse">
                                        <div className="accordion-body ff-secondary">
                                            A story can have as many themes as the reader can identify based on recurring patterns and parallels within the story
                                            itself. In looking at ways to separate themes into a hierarchy, we might find it useful to follow the example of a
                                            single book.
                                        </div>
                                    </Collapse>
                                </div>
                            </div>

                            {/* <!--end accordion--> */}
                        </Col>
                    </Row>
                </Container>
            </section>
        </React.Fragment>
    );
};

export default Faqs;