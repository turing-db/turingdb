import React from 'react';
import { Badge, Button, Card, CardBody, CardHeader, Col, Container, Row, } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

import { DisplayHeadingExample, HeadingsExample, BlockquotesExample, BlockquoteColorExample, BlockquoteBorderColorExample, InlineTextExample, UnstyleListExample, InlineListExample, DescriptionListExample, TextWrappingExample, FontSizeExample, WordBreakExample, FontWeightExample, LineHeightExample, TextTransformExample, TextDecrationExample, TextTruncationExample, VisibilityExample, ClearfixExample } from './UiTypographyCode';

const UiTypography = () => {
    document.title="Typography | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Typography" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Font Family</h4>
                                </CardHeader>

                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col sm={6}>
                                                <div className="text-muted">
                                                    <p>Body Font Family</p>
                                                    <div>
                                                        <p className="display-4 text-dark fw-medium">Aa</p>
                                                    </div>
                                                    <div>
                                                        <p className="mb-2">Font Family</p>
                                                        <h5 className="mb-0">"IBM Plex Sans", sans-serif</h5>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col sm={6}>
                                                <div className="text-muted mt-4 mt-sm-0">
                                                    <p>Heading Font Family</p>
                                                    <div>
                                                        <h1 className="display-4 text-dark fw-medium">Aa</h1>
                                                    </div>
                                                    <div>
                                                        <p className="mb-2">Font Family</p>
                                                        <h5 className="mb-0">"IBM Plex Sans", sans-serif</h5>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                </CardBody>
                            </Card>

                            <Card>
                                <PreviewCardHeader title="Headings" />
                                <CardBody>

                                    <p className="text-muted">All HTML headings, <code>&lt;h1&gt;</code> through <code>&lt;h6&gt;</code>, are available.</p>

                                    <div className="live-preview">
                                        <h1 className="mb-3">h1. Bootstrap heading <small className="text-muted">Semibold 2.03125rem (32.5px)</small></h1>
                                        <h2 className="mb-3">h2. Bootstrap heading <small className="text-muted">Semibold 1.625rem (26px)</small></h2>
                                        <h3 className="mb-3">h3. Bootstrap heading <small className="text-muted">Semibold 1.42188rem (22.8px)</small></h3>
                                        <h4 className="mb-3">h4. Bootstrap heading <small className="text-muted">Semibold 1.21875rem (19.5px)</small></h4>
                                        <h5 className="mb-3">h5. Bootstrap heading <small className="text-muted">Semibold 1.01563rem (16.25px)</small></h5>
                                        <h6 className="mb-1">h6. Bootstrap heading <small className="text-muted">Semibold 0.8125rem (13px)</small></h6>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <HeadingsExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Display Headings" />

                                <CardBody>

                                    <p className="text-muted">Traditional heading elements are designed to work best in the meat of your page content.</p>

                                    <div className="live-preview">
                                        <h1 className="display-1 mb-4">Display 1</h1>
                                        <h1 className="display-2 mb-4">Display 2</h1>
                                        <h1 className="display-3 mb-4">Display 3</h1>
                                        <h1 className="display-4 mb-4">Display 4</h1>
                                        <h1 className="display-5 mb-4">Display 5</h1>
                                        <h1 className="display-6 mb-0">Display 6</h1>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <DisplayHeadingExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Blockquotes" />

                                <CardBody>
                                    <p className="text-muted">Use <code>&lt;blockquote
                                        className="blockquote"&gt;</code> class for quoting blocks of content from
                                        another source within your document .</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xxl={6}>
                                                <div>
                                                    <blockquote className="blockquote fs-15 mb-0">
                                                        <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer posuere erat a ante.</p>
                                                        <footer className="blockquote-footer mt-0">Someone famous in <cite title="Source Title">Source Title</cite></footer>
                                                    </blockquote>
                                                </div>
                                            </Col>
                                            <Col xxl={6}>
                                                <div className="mt-4 mt-lg-0">
                                                    <blockquote className="blockquote blockquote-reverse fs-15 mb-0">
                                                        <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer posuere erat a ante.</p>
                                                        <footer className="blockquote-footer mt-0">Someone famous in <cite title="Source Title">Source Title</cite></footer>
                                                    </blockquote>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <BlockquotesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Blockquote Background Color" />
                                <CardBody>
                                    <p className="text-muted text-muted">Use <code>blockquote-</code> class with the below-mentioned color variation to set the blockquote background color.</p>
                                    <div className="live-preview">
                                        <div className="row gy-4">
                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-primary rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Primary Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-secondary rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Secondary Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-success rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Success Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-info rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Info Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-danger rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Danger Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-warning rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Warning Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-dark rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Dark Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BlockquoteColorExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Blockquote Border Color" />
                                <CardBody>
                                    <p className="text-muted text-muted">Use <code>blockquote-outline</code> class to set blockquote border color.</p>
                                    <div className="live-preview">
                                        <div className="row gy-4">

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-primary rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Primary Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-secondary rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Secondary Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-success rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Success Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-info rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Info Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-danger rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Danger Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-warning rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Warning Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>

                                            <div className="col-xl-4 col-md-6">
                                                <blockquote className="blockquote custom-blockquote blockquote-outline blockquote-dark rounded mb-0">
                                                    <p className="text-dark mb-2">Custom Blockquote Outline Dark Example</p>
                                                    <footer className="blockquote-footer mt-0">Angie Burt <cite title="Source Title">Designer</cite></footer>
                                                </blockquote>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BlockquoteBorderColorExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Inline Text Elements" />

                                <CardBody>
                                    <p className="text-muted">Styling for inline HTML5 elements.</p>
                                    <div className="live-preview">
                                        <p className="lead">
                                            Vivamus sagittis lacus vel augue laoreet rutrum faucibus dolor auctor.
                                        </p>
                                        <p>You can use the mark tag to <mark>highlight</mark> text.</p>
                                        <p><del>This line of text is meant to be treated as deleted text.</del></p>
                                        <p><s>This line of text is meant to be treated as no longer accurate.</s></p>
                                        <p><ins>This line of text is meant to be treated as an addition to the document.</ins></p>
                                        <p><u>This line of text will render as underlined</u></p>
                                        <p><small>This line of text is meant to be treated as fine print.</small></p>
                                        <p><strong>This line rendered as bold text.</strong></p>
                                        <p className="mb-0"><em>This line rendered as italicized text.</em></p>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <InlineTextExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Unstyled List" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-unstyled</code> class Remove the default list-style and left margin on list
                                        items (immediate children only). <strong>This only applies to immediate
                                            children list items</strong>, meaning you will need to add the class
                                        for any nested lists.</p>

                                    <div className="live-preview">
                                        <ul className="list-unstyled mb-0">
                                            <li>Integer molestie lorem at massa</li>
                                            <li>Nulla volutpat aliquam velit
                                                <ul>
                                                    <li>Phasellus iaculis neque</li>
                                                    <li>Purus sodales ultricies</li>
                                                    <li>Vestibulum laoreet porttitor sem</li>
                                                </ul>
                                            </li>
                                            <li>Faucibus porta lacus fringilla vel</li>
                                        </ul>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <UnstyleListExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>

                            <Card>
                                <PreviewCardHeader title="Inline List" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-inline</code> and
                                        <code> list-inline-item</code> class combination to remove a list’s bullets and apply some
                                        light margin.
                                    </p>

                                    <div className="live-preview">
                                        <ul className="list-inline mb-0">
                                            <li className="list-inline-item">Lorem ipsum</li>
                                            <li className="list-inline-item">Phasellus iaculis</li>
                                            <li className="list-inline-item">Nulla volutpat</li>
                                        </ul>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <InlineListExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <div className="col-xl-12">
                            <Card>
                                <PreviewCardHeader title="Description List Alignment" />
                                <CardBody>
                                    <p className="text-muted">Align terms and descriptions
                                        horizontally by using our grid system’s predefined classes (or semantic
                                        mixins). For longer terms, you can optionally add a <code>.text-truncate</code> class to
                                        truncate the text with an ellipsis.</p>

                                    <div className="live-preview">
                                        <dl className="row mb-0">
                                            <dt className="col-sm-3">Description lists</dt>
                                            <dd className="col-sm-9">A description list is perfect for defining terms.</dd>

                                            <dt className="col-sm-3">Euismod</dt>
                                            <dd className="col-sm-9">Vestibulum id ligula porta felis euismod semper eget lacinia odio sem nec elit.</dd>
                                            <dd className="col-sm-9 offset-sm-3">Donec id elit non mi porta gravida at eget metus.</dd>

                                            <dt className="col-sm-3">Malesuada porta</dt>
                                            <dd className="col-sm-9">Etiam porta sem malesuada magna mollis euismod.</dd>

                                            <dt className="col-sm-3 text-truncate">Truncated term is truncated</dt>
                                            <dd className="col-sm-9">Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus.</dd>

                                            <dt className="col-sm-3">Nesting</dt>
                                            <dd className="col-sm-9 mb-0">
                                                <dl className="row mb-0">
                                                    <dt className="col-sm-4">Nested definition list</dt>
                                                    <dd className="col-sm-8">Aenean posuere, tortor sed cursus feugiat, nunc augue blandit nunc.</dd>
                                                </dl>
                                            </dd>
                                        </dl>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DescriptionListExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </div>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Text Wrapping and Overflow" />

                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <p className="text-muted">Use <code>text-wrap</code> class to wrap the text.</p>
                                            <Badge color="primary" className="text-wrap" style={{ width: "6rem" }}>
                                                This text should wrap.
                                            </Badge>
                                        </div>
                                        <div className="mt-4">
                                            <p className="text-muted">Use <code>text-nowrap</code> class to prevent text from wrapping.</p>
                                            <div className="text-nowrap border bg-light mt-3" style={{ width: "8rem" }}>
                                                This text should overflow the parent.
                                            </div>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <TextWrappingExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>

                            <Card>
                                <PreviewCardHeader title="Word Break" />
                                <CardBody>
                                    <div className="live-preview">
                                        <div>
                                            <p className="text-muted">Use <code>text-break</code> class to prevent long strings of text from breaking your components' layout.</p>
                                            <p className="text-break mb-0">mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm</p>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <WordBreakExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Font size" />
                                <CardBody>
                                    <p className="text-muted">Use <code>fs-1</code>, <code>fs-2</code>, <code>fs-3</code>, <code>fs-4</code>, <code>fs-5</code>, or <code>fs-6</code>, class to change the font-size respectively.</p>
                                    <div className="live-preview">
                                        <p className="fs-1">.fs-1 text</p>
                                        <p className="fs-2">.fs-2 text</p>
                                        <p className="fs-3">.fs-3 text</p>
                                        <p className="fs-4">.fs-4 text</p>
                                        <p className="fs-5">.fs-5 text</p>
                                        <p className="fs-6">.fs-6 text</p>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <FontSizeExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Font weight and italics" />

                                <CardBody>
                                    <p className="text-muted">Use <code>fst-</code>with modifier class to change font-style and Use <code>fw-</code>with modifier class to change font-weight.</p>

                                    <div className="live-preview">
                                        <p className="fw-bold">Bold text.</p>
                                        <p className="fw-bolder">Bolder weight text (relative to the parent element).</p>
                                        <p className="fw-semibold">Semibold weight text.</p>
                                        <p className="fw-normal">Normal weight text.</p>
                                        <p className="fw-light">Light weight text.</p>
                                        <p className="fw-lighter">Lighter weight text (relative to the parent element).</p>
                                        <p className="fst-italic">Italic text.</p>
                                        <p className="fst-normal mb-0">Text with normal font style</p>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <FontWeightExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Line height" />
                                <CardBody>
                                    <p className="text-muted">Use <code>lh-</code> with modifier class to change the line height of an element.</p>

                                    <div className="live-preview">
                                        <p className="lh-1">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element. These classes can be customized as needed with our utility API.</p>
                                        <p className="lh-sm">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element. These classes can be customized as needed with our utility API.</p>
                                        <p className="lh-base">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element.</p>
                                        <p className="lh-lg mb-0">This is a long paragraph written to show how the line-height of an element is affected by our utilities. Classes are applied to the element itself or sometimes the parent element.</p>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <LineHeightExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Text Transform" />
                                <CardBody>
                                    <p className="text-muted">Use <code>text-lowercase</code>, <code>text-uppercase</code>, or <code>text-capitalize</code> to transform the text.</p>
                                    <div className="live-preview">
                                        <p className="text-lowercase">Lowercased text.</p>
                                        <p className="text-uppercase">Uppercased text.</p>
                                        <p className="text-capitalize mb-0">CapiTaliZed text.</p>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <TextTransformExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Text Decoration" />

                                <CardBody>
                                    <p className="text-muted">Use <code>text-decoration-underline</code>, <code>text-decoration-line-through</code>, or <code>text-decoration-none</code> class to decorate text in components respectively.</p>
                                    <div className="live-preview">
                                        <p className="text-decoration-underline">This text has a line underneath it.</p>
                                        <p className="text-decoration-line-through">This text has a line going through it.</p>
                                        <Link to="#" className="text-decoration-none">This link has its text decoration removed</Link>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <TextDecrationExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Text Truncation" />

                                <CardBody>
                                    <p className="text-muted">Use <code>text-truncate</code> class to truncate the text with an ellipsis. Requires <code>display: inline-block</code> or <code>display: block</code>.</p>

                                    <div className="live-preview">

                                        {/* <!-- Block level --> */}
                                        <Row>
                                            <div className="col-2 text-truncate">
                                                This text is quite long, and will be truncated once displayed.
                                            </div>
                                        </Row>

                                        {/* <!-- Inline level --> */}
                                        <span className="d-inline-block text-truncate" style={{ maxWidth: "150px" }}>
                                            This text is quite long, and will be truncated once displayed.
                                        </span>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <TextTruncationExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Visibility" />
                                <CardBody>
                                    <p className="text-muted">Use <code>visible</code> or <code>invisible</code> class to show or to hide elements respectively.</p>

                                    <div className="live-preview">
                                        <div className="visible">text visible Lorem ipsum dolor sit amet</div>
                                        <div className="invisible">text invisible Lorem ipsum dolor sit amet</div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <VisibilityExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Clearfix" />
                                <CardBody>
                                    <p className="text-muted">Use <code>clearfix</code> class to clear/set floated content within a container.</p>
                                    <div className="live-preview">
                                        <div className="bg-light clearfix p-3">
                                            <Button color="secondary" className="float-start">Example Button floated left</Button>
                                            <Button color="success" className="float-end">Example Button floated right</Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ClearfixExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment >
    );
};

export default UiTypography;
