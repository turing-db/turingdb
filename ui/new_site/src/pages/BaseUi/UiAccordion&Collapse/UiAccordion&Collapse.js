import React, { useState } from 'react';
import classnames from "classnames";
import { Accordion, AccordionItem, Button, Card, CardBody, Col, Collapse, Container, Row } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Content
import UiContent from '../../../Components/Common/UiContent';

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import { DefaultAccordionExample, FlushAccordionExample, IconAccordionExample, WithIconAccordionExample, PlusIconAccordionExample, LeftIconAccordionExample, BorderedAccordionExample, NestingAccordionExample, FillColoredAccordionExample, CollapseExample, HorizontalCollapseExample, IconCollapseExample, InlineBLockCollapseExample, MultipleTargetCollapseExample } from './UiAccordion&CollapseCode';

const UiAccordions = () => {

    document.title="Accordian & Collapse | Velzon - React Admin & Dashboard Template";

    // Default Accordion
    const [col1, setcol1] = useState(true);
    const [col2, setcol2] = useState(false);
    const [col3, setcol3] = useState(false);

    const t_col1 = () => {
        setcol1(!col1);
        setcol2(false);
        setcol3(false);
    };

    const t_col2 = () => {
        setcol2(!col2);
        setcol1(false);
        setcol3(false);
    };

    const t_col3 = () => {
        setcol3(!col3);
        setcol1(false);
        setcol2(false);
    };

    // Accordion Flush
    const [col4, setcol4] = useState(true);
    const [col5, setcol5] = useState(false);
    const [col6, setcol6] = useState(false);


    const t_col4 = () => {
        setcol4(!col4);
        setcol5(false);
        setcol6(false);
    };

    const t_col5 = () => {
        setcol5(!col5);
        setcol4(false);
        setcol6(false);
    };

    const t_col6 = () => {
        setcol6(!col6);
        setcol4(false);
        setcol5(false);
    };

    // Accordions with Icons
    const [iconCol1, seticonCol1] = useState(true);
    const [iconCol2, seticonCol2] = useState(false);
    const [iconCol3, seticonCol3] = useState(false);


    const t_iconCol1 = () => {
        seticonCol1(!iconCol1);
        seticonCol2(false);
        seticonCol3(false);
    };

    const t_iconCol2 = () => {
        seticonCol2(!iconCol2);
        seticonCol1(false);
        seticonCol3(false);
    };

    const t_iconCol3 = () => {
        seticonCol3(!iconCol3);
        seticonCol1(false);
        seticonCol2(false);
    };

    // Accordions without Icons
    const [iconCol4, seticonCol4] = useState(true);
    const [iconCol5, seticonCol5] = useState(false);
    const [iconCol6, seticonCol6] = useState(false);


    const t_iconCol4 = () => {
        seticonCol4(!iconCol4);
        seticonCol5(false);
        seticonCol6(false);
    };

    const t_iconCol5 = () => {
        seticonCol5(!iconCol5);
        seticonCol4(false);
        seticonCol6(false);
    };

    const t_iconCol6 = () => {
        seticonCol6(!iconCol6);
        seticonCol4(false);
        seticonCol5(false);
    };

    // Accordions with Plus Icon
    const [plusiconCol1, setplusiconCol1] = useState(true);
    const [plusiconCol2, setplusiconCol2] = useState(false);
    const [plusiconCol3, setplusiconCol3] = useState(false);


    const t_plusiconCol1 = () => {
        setplusiconCol1(!plusiconCol1);
        setplusiconCol2(false);
        setplusiconCol3(false);
    };

    const t_plusiconCol2 = () => {
        setplusiconCol2(!plusiconCol2);
        setplusiconCol1(false);
        setplusiconCol3(false);
    };

    const t_plusiconCol3 = () => {
        setplusiconCol3(!plusiconCol3);
        setplusiconCol1(false);
        setplusiconCol2(false);
    };

    // Left Icon Accordions
    const [lefticonCol1, setlefticonCol1] = useState(true);
    const [lefticonCol2, setlefticonCol2] = useState(false);
    const [lefticonCol3, setlefticonCol3] = useState(false);


    const t_lefticonCol1 = () => {
        setlefticonCol1(!lefticonCol1);
        setlefticonCol2(false);
        setlefticonCol3(false);
    };

    const t_lefticonCol2 = () => {
        setlefticonCol2(!lefticonCol2);
        setlefticonCol1(false);
        setlefticonCol3(false);
    };

    const t_lefticonCol3 = () => {
        setlefticonCol3(!lefticonCol3);
        setlefticonCol1(false);
        setlefticonCol2(false);
    };


    // Accordions Bordered
    const [borderCol1, setborderCol1] = useState(true);
    const [borderCol2, setborderCol2] = useState(false);
    const [borderCol3, setborderCol3] = useState(false);

    const t_borderCol1 = () => {
        setborderCol1(!borderCol1);
        setborderCol2(false);
        setborderCol3(false);
    };

    const t_borderCol2 = () => {
        setborderCol2(!borderCol2);
        setborderCol1(false);
        setborderCol3(false);
    };

    const t_borderCol3 = () => {
        setborderCol3(!borderCol3);
        setborderCol1(false);
        setborderCol2(false);
    };

    // Nesting Accordions
    const [nestingCol1, setnestingCol1] = useState(true);
    const [nestingCol2, setnestingCol2] = useState(false);
    const [nestingCol3, setnestingCol3] = useState(false);


    const t_nestingCol1 = () => {
        setnestingCol1(!nestingCol1);
        setnestingCol2(false);
        setnestingCol3(false);
    };

    const t_nestingCol2 = () => {
        setnestingCol2(!nestingCol2);
        setnestingCol1(false);
        setnestingCol3(false);
    };

    const t_nestingCol3 = () => {
        setnestingCol3(!nestingCol3);
        setnestingCol1(false);
        setnestingCol2(false);
    };

    // level First Nesting
    const [anestingCol1, setanestingCol1] = useState(true);
    const [anestingCol2, setanestingCol2] = useState(false);


    const t_anestingCol1 = () => {
        setanestingCol1(!anestingCol1);
        setanestingCol2(false);
    };

    const t_anestingCol2 = () => {
        setanestingCol2(!anestingCol2);
        setanestingCol1(false);
    };

    // level Second Nesting
    const [bnestingCol1, setbnestingCol1] = useState(false);


    const t_bnestingCol1 = () => {
        setbnestingCol1(!bnestingCol1);
    };

    // level Second's First Nesting
    const [a1nestingCol1, seta1nestingCol1] = useState(true);


    const t_a1nestingCol1 = () => {
        seta1nestingCol1(!a1nestingCol1);
    };


    // Accordions Fill Colored
    const [fillCol1, setfillCol1] = useState(true);
    const [fillCol2, setfillCol2] = useState(false);
    const [fillCol3, setfillCol3] = useState(false);


    const t_fillCol1 = () => {
        setfillCol1(!fillCol1);
        setfillCol2(false);
        setfillCol3(false);
    };

    const t_fillCol2 = () => {
        setfillCol2(!fillCol2);
        setfillCol1(false);
        setfillCol3(false);
    };

    const t_fillCol3 = () => {
        setfillCol3(!fillCol3);
        setfillCol1(false);
        setfillCol2(false);
    };

    const [fillCol4, setfillCol4] = useState(true);
    const [fillCol5, setfillCol5] = useState(false);
    const [fillCol6, setfillCol6] = useState(false);


    const t_fillCol4 = () => {
        setfillCol4(!fillCol4);
        setfillCol5(false);
        setfillCol6(false);
    };

    const t_fillCol5 = () => {
        setfillCol5(!fillCol5);
        setfillCol4(false);
        setfillCol6(false);
    };

    const t_fillCol6 = () => {
        setfillCol6(!fillCol6);
        setfillCol4(false);
        setfillCol5(false);
    };

    // Collapse Example

    const [coll1, setcoll1] = useState(true);

    const t_coll1 = () => {
        setcoll1(!coll1);
    };

    // Horizontal Collapse

    const [coll2, setcoll2] = useState(true);

    const t_coll2 = () => {
        setcoll2(!coll2);
    };

    // Multiple Targets Collapse

    const [coll3, setcoll3] = useState(true);
    const [coll4, setcoll4] = useState(true);


    const t_coll3 = () => {
        setcoll3(!coll3);
    };

    const t_coll4 = () => {
        setcoll4(!coll4);
    };

    const t_coll5 = () => {
        setcoll3(!coll3);
        setcoll4(!coll4);
    };

    // Collapse with Icon

    const [coll6, setcoll6] = useState(true);
    const [coll7, setcoll7] = useState(false);

    const t_coll6 = () => {
        setcoll6(!coll6);
    };

    const t_coll7 = () => {
        setcoll7(!coll7);
    };

    // Inline & Block Element Collapse

    const [coll8, setcoll8] = useState(true);
    const [coll9, setcoll9] = useState(true);

    const t_coll8 = () => {
        setcoll8(!coll8);
    };

    const t_coll9 = () => {
        setcoll9(!coll9);
    };

    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Accordions" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Default Accordion" />

                                <CardBody>
                                    <p className="text-muted">Use the <code>accordion</code>class to expand/collapse the accordion content.</p>
                                    <div className="live-preview">
                                        <Accordion id="default-accordion-example">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingOne">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !col1 })} type="button" onClick={t_col1} style={{ cursor: "pointer" }} >
                                                        How to create a group booking ?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={col1} className="accordion-collapse" id="collapseOne" >
                                                    <div className="accordion-body">
                                                        Although you probably won’t get into any legal trouble if you do it just once, why risk it? If you made your subscribers a promise, you should honor that. If not, you run the risk of a drastic increase in opt outs, which will only hurt you in the long run.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingTwo">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !col2 })} type="button" onClick={t_col2} style={{ cursor: "pointer" }} >
                                                        Why do we use it ?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={col2} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        No charges are put in place by SlickText when subscribers join your text list. This does not mean however that charges 100% will not occur. Charges that may occur fall under part of the compliance statement stating "Message and Data rates may apply."
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingThree">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !col3 })} type="button" onClick={t_col3} style={{ cursor: "pointer" }} >
                                                        Where does it come from ?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={col3} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        Now that you have a general idea of the amount of texts you will need per month, simply find a plan size that allows you to have this allotment, plus some extra for growth. Don't worry, there are no mistakes to be made here. You can always upgrade and downgrade.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DefaultAccordionExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Accordion Flush" />

                                <CardBody>
                                    <p className="text-muted">Use <code>accordion-flush</code> class to remove the default background-color, some borders, and some rounded corners to render accordions edge-to-edge with their parent container.</p>
                                    <div className="live-preview">

                                        <Accordion id="default-accordion-example" flush>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingOne">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !col4 })} type="button" onClick={t_col4} style={{ cursor: "pointer" }} >
                                                        How do I set up my profile ?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={col4} className="accordion-collapse" id="collapseOne" >
                                                    <div className="accordion-body">
                                                        The renewal of your SlickText service happens on the anniversary of your original paid sign up date. Upgrading in the middle of your billing period will not change the billing date. Upgrading does however force an immediate, pro-rated charge to take place on your account.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingTwo">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !col5 })} type="button" onClick={t_col5} style={{ cursor: "pointer" }} >
                                                        What can I do with my project ?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={col5} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        If you had signed up on a free account with Slicktext, then upgraded to a paid plan at a later date, your bill will renew based on the date you had upgraded to a paid plan. All invoices are able to be viewed under your plan options in your SlickText account.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingThree">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !col6 })} type="button" onClick={t_col6} style={{ cursor: "pointer" }} >
                                                        Where can I go to edit voice settings
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={col6} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        No, we cannot provide this information. Opting out from a list is an anonymous, private act. This prevents further harassment. Providing this information is considered bad practice, and further communication after they opt out would be considered against compliance.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <FlushAccordionExample />
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
                                <PreviewCardHeader title="Accordions with Icons" />
                                <CardBody>
                                    <p className="text-muted">Use <code>custom-accordionwithicon</code> class to show custom icon at accordion.</p>
                                    <div className="live-preview">

                                        <Accordion className="custom-accordionwithicon accordion-secondary" id="accordionWithicon">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingOne">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !iconCol1 })} type="button" onClick={t_iconCol1} style={{ cursor: "pointer" }} >
                                                        <i className="ri-global-line me-2"></i> How Does Age Verification Work?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={iconCol1} className="accordion-collapse" id="collapseOne" >
                                                    <div className="accordion-body">
                                                        Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna elements, buttons, everything.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingTwo">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !iconCol2 })} type="button" onClick={t_iconCol2} style={{ cursor: "pointer" }} >
                                                        <i className="ri-user-location-line me-2"></i> How Does Link Tracking Work?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={iconCol2} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingThree">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !iconCol3 })} type="button" onClick={t_iconCol3} style={{ cursor: "pointer" }} >
                                                        <i className="ri-pen-nib-line me-2"></i> How Do I Set Up the Drip Feature?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={iconCol3} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <IconAccordionExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Accordions without Icons" />
                                <CardBody>
                                    <p className="text-muted">Use <code>accordion-icon-none</code> class to remove icon at accordion.</p>
                                    <div className="live-preview">

                                        <Accordion className="accordion-icon-none" id="accordionWithouticon">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionwithouticonExample1">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !iconCol4 })} type="button" onClick={t_iconCol4} style={{ cursor: "pointer" }} >
                                                        <i className="ri-global-line me-2"></i> How Does Age Verification Work?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={iconCol4} className="accordion-collapse" id="accor_withouticoncollapse1" >
                                                    <div className="accordion-body">
                                                        Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your own style choices. For that very reason, I went on a quest and spoke to many different professional graphic designers.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionwithouticonExample2">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !iconCol5 })} type="button" onClick={t_iconCol5} style={{ cursor: "pointer" }} >
                                                        <i className="ri-user-location-line me-2"></i> How Does Link Tracking Work?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={iconCol5} className="accordion-collapse" id="accor_withouticoncollapse2" >
                                                    <div className="accordion-body">
                                                        When, while the lovely valley teems with vapour around me, and the meridian sun strikes the upper surface of the impenetrable foliage of my trees, and but a few stray gleams steal into the inner sanctuary, I throw myself down among the tall grass by the trickling stream; and, as I lie close to the earth, a thousand unknown.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionwithouticonExample3">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !iconCol6 })} type="button" onClick={t_iconCol6} style={{ cursor: "pointer" }} >
                                                        <i className="ri-pen-nib-line me-2"></i> How Do I Set Up the Drip Feature?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={iconCol6} className="accordion-collapse" id="accor_withouticoncollapse3" >
                                                    <div className="accordion-body">
                                                        Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <WithIconAccordionExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={12}>
                            <Card>

                                <PreviewCardHeader title="Accordions with Plus Icon" />

                                <CardBody>
                                    <p className="text-muted">Use <code>custom-accordionwithicon-plus</code> class to show plus icon at the accordion.</p>
                                    <div className="live-preview">

                                        <Accordion className="custom-accordionwithicon-plus" id="accordionWithplusicon">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionwithouticonExample1">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !plusiconCol1 })} type="button" onClick={t_plusiconCol1} style={{ cursor: "pointer" }} >
                                                        What is User Interface Design?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={plusiconCol1} className="accordion-collapse" id="accor_plusExamplecollapse1" >
                                                    <div className="accordion-body">
                                                        Big July earthquakes confound zany experimental vow. My girl wove six dozen plaid jackets before she quit. Six big devils from Japan quickly forgot how to waltz. try again until it looks right, and each assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna elements, buttons, everything.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionwithplusExample2">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !plusiconCol2 })} type="button" onClick={t_plusiconCol2} style={{ cursor: "pointer" }} >
                                                        What is Digital Marketing?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={plusiconCol2} className="accordion-collapse" id="accor_plusExamplecollapse2" >
                                                    <div className="accordion-body">
                                                        It makes a statement, it’s impressive graphic design. Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each letter has the perfect spot of its own. commodo enim craft beer mlkshk aliquip jean shorts ullamco.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionwithplusExample3">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !plusiconCol3 })} type="button" onClick={t_plusiconCol3} style={{ cursor: "pointer" }} >
                                                        Where does it come from ?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={plusiconCol3} className="accordion-collapse" id="accor_plusExamplecollapse3" >
                                                    <div className="accordion-body">
                                                        Spacing depending on the situation and try, try again until it looks right, and each. next level wes anderson artisan four loko farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco. omo nostrud organic, assumenda labore aesthetic magna delectus. pposites attract, and that’s a fact.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <PlusIconAccordionExample />
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

                                <PreviewCardHeader title="Left Icon Accordions" />

                                <CardBody>
                                    <p className="text-muted">Use <code>lefticon-accordion</code> class to show the icon on the left side of the accordion.</p>
                                    <div className="live-preview">
                                        <Accordion className="lefticon-accordion custom-accordionwithicon accordion-border-box" id="accordionlefticon">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionlefticonExample1">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !lefticonCol1 })} type="button" onClick={t_lefticonCol1} style={{ cursor: "pointer" }} >
                                                        What is User Interface Design?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={lefticonCol1} className="accordion-collapse" id="accor_lefticonExamplecollapse1" >
                                                    <div className="accordion-body">
                                                        Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua nulla assumenda shoreditch et.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionlefticonExample2">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !lefticonCol2 })} type="button" onClick={t_lefticonCol2} style={{ cursor: "pointer" }} >
                                                        What is Digital Marketing?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={lefticonCol2} className="accordion-collapse" id="accor_lefticonExamplecollapse2" >
                                                    <div className="accordion-body">
                                                        Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionlefticonExample3">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !lefticonCol3 })} type="button" onClick={t_lefticonCol3} style={{ cursor: "pointer" }} >
                                                        Where does it come from ?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={lefticonCol3} className="accordion-collapse" id="accor_lefticonExamplecollapse3" >
                                                    <div className="accordion-body">
                                                        Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <LeftIconAccordionExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Accordions Bordered" />

                                <CardBody>
                                    <p className="text-muted">Use <code>custom-accordion-border</code> class to create the border at the accordion.</p>
                                    <div className="live-preview">

                                        <Accordion className="custom-accordionwithicon custom-accordion-border accordion-border-box accordion-success" id="accordionBordered">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionborderedExample1">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !borderCol1 })} type="button" onClick={t_borderCol1} style={{ cursor: "pointer" }} >
                                                        What is User Interface Design?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={borderCol1} className="accordion-collapse" id="accor_borderedExamplecollapse1" >
                                                    <div className="accordion-body">
                                                        Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua nulla assumenda shoreditch et.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionborderedExample2">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !borderCol2 })} type="button" onClick={t_borderCol2} style={{ cursor: "pointer" }} >
                                                        What is Digital Marketing?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={borderCol2} className="accordion-collapse" id="accor_borderedExamplecollapse2" >
                                                    <div className="accordion-body">
                                                        Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionborderedExample3">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !borderCol3 })} type="button" onClick={t_borderCol3} style={{ cursor: "pointer" }} >
                                                        Where does it come from ?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={borderCol3} className="accordion-collapse" id="accor_borderedExamplecollapse3" >
                                                    <div className="accordion-body">
                                                        Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BorderedAccordionExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={12}>
                            <Card>

                                <PreviewCardHeader title="Nesting Accordions" />

                                <CardBody>
                                    <p className="text-muted">Use <code>nesting-accordion</code> class to create a nesting accordion.</p>
                                    <div className="live-preview">

                                        <Accordion className="custom-accordionwithicon accordion-border-box" id="accordionnesting">
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="accordionnestingExample1">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !nestingCol1 })} type="button" onClick={t_nestingCol1} style={{ cursor: "pointer" }} >
                                                        How Do I Add Contacts/Subscribers?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={nestingCol1} className="accordion-collapse" id="accor_nestingExamplecollapse1" >
                                                    <div className="accordion-body">
                                                        This opt in method is perfect for those looking to integrate a different software such Shopify or Hubspot with Slicktext. For example, upon cashing out online, a subscriber may submit to have their mobile number to receive marketing messages. The API will pass this information from said software over to Slicktext via API integration.
                                                        <div className="accordion nesting2-accordion custom-accordionwithicon accordion-border-box mt-3" id="accordionnesting2">
                                                            <AccordionItem>
                                                                <h2 className="accordion-header" id="accordionnesting2Example1">
                                                                    <button
                                                                        className={classnames("accordion-button", { collapsed: !anestingCol1 })} type="button" onClick={t_anestingCol1} style={{ cursor: "pointer" }} >
                                                                        How Do I Search For Contacts?
                                                                    </button>
                                                                </h2>

                                                                <Collapse isOpen={anestingCol1} className="accordion-collapse" id="accor_nesting2Examplecollapse1" >
                                                                    <div className="accordion-body">
                                                                        When you are in need of finding a specific subscriber, you will want to use the help of SlickText's search navigation tool, located under the Contacts tab of your Slicktext account. Once here, you will have options to sort by, filter, and search your contacts.
                                                                        <div className="accordion nesting4-accordion custom-accordionwithicon accordion-border-box mt-3" id="accordionnesting4">
                                                                            <AccordionItem>
                                                                                <h2 className="accordion-header" id="accordionnesting4Example2">
                                                                                    <button
                                                                                        className={classnames("accordion-button", { collapsed: !bnestingCol1 })} type="button" onClick={t_bnestingCol1} style={{ cursor: "pointer" }} >
                                                                                        How do I reset my digital tide watch ?
                                                                                    </button>
                                                                                </h2>

                                                                                <Collapse isOpen={bnestingCol1} className="accordion-collapse" id="accor_nesting4Examplecollapse2" >
                                                                                    <div className="accordion-body">
                                                                                        Opting out a subscriber will allow SlickText to maintain the history of the subscriber as it pertains to the textword you are opting them out of. If this user was to text to join again, the initial text they are met with will be a "welcome back" message instead of the auto reply.
                                                                                    </div>
                                                                                </Collapse>
                                                                            </AccordionItem>
                                                                        </div>
                                                                    </div>
                                                                </Collapse>
                                                            </AccordionItem>
                                                            <AccordionItem>
                                                                <h2 className="accordion-header" id="accordionnesting2Example2">
                                                                    <button
                                                                        className={classnames("accordion-button", { collapsed: !anestingCol2 })} type="button" onClick={t_anestingCol2} style={{ cursor: "pointer" }} >
                                                                        How Do I Delete a Contact From My List?
                                                                    </button>
                                                                </h2>

                                                                <Collapse isOpen={anestingCol2} className="accordion-collapse" >
                                                                    <div className="accordion-body">
                                                                        Opting out a subscriber will allow SlickText to maintain the history of the subscriber as it pertains to the textword you are opting them out of. If this user was to text to join again, the initial text they are met with will be a "welcome back" message instead of the auto reply.
                                                                    </div>
                                                                </Collapse>
                                                            </AccordionItem>

                                                        </div>
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingTwo">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !nestingCol2 })} type="button" onClick={t_nestingCol2} style={{ cursor: "pointer" }} >
                                                        How Does Personalization Work?
                                                    </button>
                                                </h2>

                                                <Collapse isOpen={nestingCol2} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        Personalization allows you to provide a personal touch to your outbound text marketing campaigns. SlickText uses merge tags as placeholders that are replaced with contact-specific information when a text message is sent. These merge tags may also be known as personalization fields.
                                                        <div className="accordion nesting3-accordion custom-accordionwithicon accordion-border-box mt-3" id="accordionnesting3">
                                                            <div className="accordion-item mt-2">
                                                                <h2 className="accordion-header" id="accordionnesting4Example2">
                                                                    <button
                                                                        className={classnames("accordion-button", { collapsed: !a1nestingCol1 })} type="button" onClick={t_a1nestingCol1} style={{ cursor: "pointer" }} >
                                                                        Howe does temperature impact my watch?
                                                                    </button>
                                                                </h2>

                                                                <Collapse isOpen={a1nestingCol1} className="accordion-collapse" id="accor_nesting3Examplecollapse2" >
                                                                    <div className="accordion-body">
                                                                        Opting out a subscriber will allow SlickText to maintain the history of the subscriber as it pertains to the textword you are opting them out of. If this user was to text to join again, the initial text they are met with will be a "welcome back" message instead of the auto reply.
                                                                    </div>
                                                                </Collapse>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                            <AccordionItem>
                                                <h2 className="accordion-header" id="headingThree">
                                                    <button
                                                        className={classnames("accordion-button", { collapsed: !nestingCol3 })} type="button" onClick={t_nestingCol3} style={{ cursor: "pointer" }} >
                                                        What Happens When I Run Out of Messages?
                                                    </button>
                                                </h2>
                                                <Collapse isOpen={nestingCol3} className="accordion-collapse" >
                                                    <div className="accordion-body">
                                                        When you run out of messages, you will not be able to send any outbound campaigns. This would include any scheduled messages, drip campaigns, and birthday messages. However, we will continue to deliver your auto-reply messages to allow your subscriber list to continue to grow.
                                                    </div>
                                                </Collapse>
                                            </AccordionItem>
                                        </Accordion>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <NestingAccordionExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={12}>
                            <Card>

                                <PreviewCardHeader title="Accordions Fill Colored" />

                                <CardBody>
                                    <p className="text-muted">Use <code>accordion-fill-</code> class with modifier class to the color accordion.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xxl={6}>
                                                <Accordion className="custom-accordionwithicon accordion-fill-success" id="accordionFill">
                                                    <AccordionItem>
                                                        <h2 className="accordion-header" id="accordionFillExample1">
                                                            <button
                                                                className={classnames("accordion-button", { collapsed: !fillCol1 })} type="button" onClick={t_fillCol1} style={{ cursor: "pointer" }} >
                                                                What are webhooks?
                                                            </button>
                                                        </h2>

                                                        <Collapse isOpen={fillCol1} className="accordion-collapse" id="accor_fill1" >
                                                            <div className="accordion-body">
                                                                Webhooks allow you to gather real time data on key interactions that happen with your Slick Text account. Simply provide us with a url where you'd like the data to be sent, choose which events you'd like to be informed of, and click save.                                                            </div>
                                                        </Collapse>
                                                    </AccordionItem>
                                                    <AccordionItem>
                                                        <h2 className="accordion-header" id="accordionFillExample2">
                                                            <button
                                                                className={classnames("accordion-button", { collapsed: !fillCol2 })} type="button" onClick={t_fillCol2} style={{ cursor: "pointer" }} >
                                                                Where can I find my Textword ID?
                                                            </button>
                                                        </h2>

                                                        <Collapse isOpen={fillCol2} className="accordion-collapse" id="accor_fill2" >
                                                            <div className="accordion-body">
                                                                Head over to the Textwords page. Click options on the right hand side, and then click Settings. This will redirect you to your Textword Setting page. Now, go check your url, and the textword ID will be the number after "word=". Too much or too little spacing, as in the example below.
                                                            </div>
                                                        </Collapse>
                                                    </AccordionItem>
                                                    <AccordionItem>
                                                        <h2 className="accordion-header" id="accordionFillExample3">
                                                            <button
                                                                className={classnames("accordion-button", { collapsed: !fillCol3 })} type="button" onClick={t_fillCol3} style={{ cursor: "pointer" }} >
                                                                Where is your API documentation?
                                                            </button>
                                                        </h2>
                                                        <Collapse isOpen={fillCol3} className="accordion-collapse" id="accor_fill3" >
                                                            <div className="accordion-body">
                                                                You always want to make sure that your fonts work well together and try to limit the number of fonts you use to three or less. Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites.
                                                            </div>
                                                        </Collapse>
                                                    </AccordionItem>
                                                </Accordion>
                                            </Col>
                                            <Col xxl={6}>

                                                <Accordion className="custom-accordionwithicon accordion-flush accordion-fill-secondary" id="accordionFill2">
                                                    <AccordionItem>
                                                        <h2 className="accordion-header" id="accordionFill2Example1">
                                                            <button
                                                                className={classnames("accordion-button", { collapsed: !fillCol4 })} type="button" onClick={t_fillCol4} style={{ cursor: "pointer" }} >
                                                                How Does Age Verification Work?
                                                            </button>
                                                        </h2>

                                                        <Collapse isOpen={fillCol4} className="accordion-collapse" id="accor_fill21" >
                                                            <div className="accordion-body">
                                                                Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid. Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR.
                                                            </div>
                                                        </Collapse>
                                                    </AccordionItem>
                                                    <AccordionItem>
                                                        <h2 className="accordion-header" id="accordionFill2Example2">
                                                            <button
                                                                className={classnames("accordion-button", { collapsed: !fillCol5 })} type="button" onClick={t_fillCol5} style={{ cursor: "pointer" }} >
                                                                What Kind of List Growth Should I Expect?
                                                            </button>
                                                        </h2>

                                                        <Collapse isOpen={fillCol5} className="accordion-collapse" id="accor_fill22" >
                                                            <div className="accordion-body">
                                                                Consistency is the one thing that can take all of the different elements in your design, and tie them all together and make them work. In an awareness campaign, it is vital for people to begin put 2 and 2 together and begin to recognize your cause. Consistency piques people’s interest.
                                                            </div>
                                                        </Collapse>
                                                    </AccordionItem>
                                                    <AccordionItem>
                                                        <h2 className="accordion-header" id="accordionFill2Example3">
                                                            <button
                                                                className={classnames("accordion-button", { collapsed: !fillCol6 })} type="button" onClick={t_fillCol6} style={{ cursor: "pointer" }} >
                                                                How Do I Delete a Contact From My List?
                                                            </button>
                                                        </h2>
                                                        <Collapse isOpen={fillCol6} className="accordion-collapse" id="accor_fill23" >
                                                            <div className="accordion-body">
                                                                Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis aliquam ultrices mauris.                                                            </div>
                                                        </Collapse>
                                                    </AccordionItem>
                                                </Accordion>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <FillColoredAccordionExample />
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

                                <PreviewCardHeader title="Collapse Example" />

                                <CardBody>                                    
                                    <div className="live-preview">
                                        <div className="d-flex gap-2 flex-wrap mb-3">
                                            <Link
                                                to="#"
                                                onClick={t_coll1}
                                                style={{ cursor: "pointer" }}
                                                className="btn btn-primary"
                                            >
                                                Link with href{" "}
                                            </Link>
                                            <Button
                                                color="primary"
                                                onClick={t_coll1}
                                                style={{ cursor: "pointer" }}
                                            >
                                                Button with data-target
                                            </Button>
                                        </div>
                                        <Collapse isOpen={coll1} id="collapseExample">
                                            <div className="card mb-0">
                                                <CardBody>
                                                    When designing, the goal is to draw someone’s attention and portray to them what you’re trying to say. You can make a big statement by using little tricks, like this one. Use contrasting fonts. you can use a bold sanserif font with a cursive.
                                                </CardBody>
                                            </div>
                                        </Collapse>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <CollapseExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Horizontal Collapse" />

                                <CardBody>                                    
                                    <div className="live-preview">
                                        <p>
                                            <Button onClick={t_coll2} color="primary" style={{ cursor: "pointer" }} > Toggle Width Collapse </Button>
                                        </p>

                                        <div>

                                            <Collapse isOpen={coll2} id="collapseWidthExample" horizontal>
                                                <div className="card card-body mb-0" style={{ width: "300px" }}>
                                                    This is some placeholder content for a horizontal collapse. It's hidden by default and shown when triggered.
                                                </div>
                                            </Collapse>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <HorizontalCollapseExample />
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

                                <PreviewCardHeader title="Collapse with Icon" />

                                <CardBody>
                                    <p className="text-muted">
                                        Here is the example of collapse in which the icon is wrapped within the button collapse toggle.
                                    </p>
                                    <div className="live-preview">

                                        <div className="hstack gap-3 mb-3">
                                            <Link to="#" onClick={t_coll6} style={{ cursor: "pointer" }} className="link-success" >
                                                <i className="ri-arrow-down-circle-line fs-16"></i>
                                            </Link>
                                            <Button color="light" onClick={t_coll7} style={{ cursor: "pointer" }} >
                                                <i className="ri-filter-2-line"></i>
                                            </Button>
                                        </div>
                                        <Collapse isOpen={coll6} id="collapseWithicon">
                                            <div className="card mb-0">
                                                <CardBody>
                                                    If you enter text including symbols in the search criteria, the search criteria is interpreted exactly as you entered it, and the search is case sensitive as a case insensitive search that contains certain text but does also provide a lot of search criteria options.
                                                </CardBody>
                                            </div>
                                        </Collapse>
                                        <Collapse isOpen={coll7} id="collapseWithicon2">
                                            <div className="card mb-0 mt-3">
                                                <CardBody>
                                                    When you want to search for data, such as customer names, addresses, or product groups, you enter criteria. In search criteria you can use all the numbers and letters that you normally use in the specific field. In addition, you can use special symbols to further filter the results.
                                                </CardBody>
                                            </div>
                                        </Collapse>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <IconCollapseExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Inline & Block Element Collapse" />

                                <CardBody>
                                    <p className="text-muted">Inline element collapse takes all horizontal space hence you can activate the collapse within by clicking on
                                        full-width horizontal space. Block element collapse can be activated by clicking on collapse toggle only.</p>
                                    <div className="live-preview">

                                        <div className="mb-3">
                                            <h6 className="text-primary" onClick={t_coll8}>
                                                <code>&lt;h6&gt;</code> Inline Element Collapse
                                            </h6>

                                            <span role="button" className="text-primary fw-medium" onClick={t_coll9}>
                                                <code>&lt;span&gt;</code> Block Element Collapse
                                            </span>

                                        </div>
                                        <Row className="g-2">
                                            <Col className="col-auto">
                                                <Collapse isOpen={coll8} id="collapseblock" horizontal>
                                                    <div className="card card-body mb-0" style={{ width: "300px" }}>
                                                        A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring heart.
                                                    </div>
                                                </Collapse>
                                            </Col>
                                            <Col className="col-auto">
                                                <Collapse isOpen={coll9} id="collapseinline" horizontal>
                                                    <div className="card card-body mb-0" style={{ width: "300px" }}>
                                                        When you have created a new account schedule and set up the rows, you must set up columns.
                                                    </div>
                                                </Collapse>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <InlineBLockCollapseExample />
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
                                <PreviewCardHeader title="Multiple Targets Collapse" />
                                <CardBody>
                                    <p className="text-muted">
                                        A <code>&lt;button&gt;</code> or <code>&lt;a&gt;</code> can show and hide multiple elements by referencing them with a selector in its href or data-bs-target attribute. Multiple <code>&lt;button&gt;</code> or <code>&lt;a&gt;</code> can
                                        show and hide an element if they each reference it with their href or data-bs-target attribute.
                                    </p>
                                    <div className="live-preview">

                                        <div className="d-flex gap-2 flex-wrap mb-3">
                                            <Link to="#" onClick={t_coll3} style={{ cursor: "pointer" }} className="btn btn-primary" > Toggle First Element </Link>
                                            <Button onClick={t_coll4} color="primary" style={{ cursor: "pointer" }} > Toggle Second Element </Button>
                                            <Button onClick={t_coll5} color="primary" style={{ cursor: "pointer" }} > Toggle Both Elements </Button>
                                        </div>
                                        <Row>
                                            <Col>
                                                <Collapse isOpen={coll3} id="multiCollapseExample1" className="multi-collapse">
                                                    <div className="card card-body mb-0">
                                                        Some placeholder content for the first collapse component of this multi-collapse example. This panel is hidden by default but revealed when the user activates the relevant trigger.
                                                    </div>
                                                </Collapse>
                                            </Col>
                                            <Col>
                                                <Collapse isOpen={coll4} id="multiCollapseExample2" className="multi-collapse">
                                                    <div className="card card-body mb-0">
                                                        Some placeholder content for the second collapse component of this multi-collapse example. This panel is hidden by default but revealed when the user activates the relevant trigger.
                                                    </div>
                                                </Collapse>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <MultipleTargetCollapseExample />
                                            </code>
                                        </pre>
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

export default UiAccordions;
