import React from 'react'
import { Card, CardBody, Col, Container, Input, Label, Row, Table } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import UiContent from "../../../Components/Common/UiContent";
import { Link } from 'react-router-dom';
import {
    DefaultTables,
    StrippedRow,
    TablesColors,
    HoverableRows,
    CardTables,
    ActiveTables,
    BorderedTables,
    TablesBorderColors,
    TablesWithoutBorders,
    SmallTables,
    TableHead,
    TableFoot,
    Captions,
    TableNesting,
    Variants,
    VerticalAlignment,
    ResponsiveTables,
    StripedColumnsTables
} from './BasicTablesCode';

//Import images
import avtarImage1 from '../../../assets/images/users/avatar-1.jpg';
import avtarImage3 from '../../../assets/images/users/avatar-3.jpg';
import avtarImage4 from '../../../assets/images/users/avatar-4.jpg';
import avtarImage5 from '../../../assets/images/users/avatar-5.jpg';
import avtarImage6 from '../../../assets/images/users/avatar-6.jpg';



const BasicTables = () => {
    document.title = "Basic Tables | Velzon - React Admin & Dashboard Template";

    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Basic Tables" pageTitle="Tables" />
                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Default Tables" />
                                <CardBody>
                                    <p className="text-muted">Use <code>Table</code> attribute to show bootstrap-based default table.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="align-middle table-nowrap mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Customer</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Invoice</th>
                                                        <th scope="col">Action</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <th scope="row"><Link to="#" className="fw-medium">#VZ2110</Link></th>
                                                        <td>Bobby Davis</td>
                                                        <td>October 15, 2021</td>
                                                        <td>$2,300</td>
                                                        <td><Link to="#" className="link-success">View More <i className="ri-arrow-right-line align-middle"></i></Link></td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row"><Link to="#" className="fw-medium">#VZ2109</Link></th>
                                                        <td>Christopher Neal</td>
                                                        <td>October 7, 2021</td>
                                                        <td>$5,500</td>
                                                        <td><Link to="#" className="link-success">View More <i className="ri-arrow-right-line align-middle"></i></Link></td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row"><Link to="#" className="fw-medium">#VZ2108</Link></th>
                                                        <td>Monkey Karry</td>
                                                        <td>October 5, 2021</td>
                                                        <td>$2,420</td>
                                                        <td><Link to="#" className="link-success">View More <i className="ri-arrow-right-line align-middle"></i></Link></td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row"><Link to="#" className="fw-medium">#VZ2107</Link></th>
                                                        <td>James White</td>
                                                        <td>October 2, 2021</td>
                                                        <td>$7,452</td>
                                                        <td><Link to="#" className="link-success">View More <i className="ri-arrow-right-line align-middle"></i></Link></td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <DefaultTables />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xl={6} >
                            <Card>
                                <PreviewCardHeader title="Striped Rows" />
                                <CardBody>
                                    <p className="text-muted">Use <code>table-striped</code> class to add zebra-striping to any table row within the &lt;tbody&gt;.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-striped table-nowrap align-middle mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Customer</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Invoice</th>
                                                        <th scope="col">Action</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Bobby Davis</td>
                                                        <td>Nov 14, 2021</td>
                                                        <td>$2,410</td>
                                                        <td><span className="badge bg-success">Confirmed</span></td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Christopher Neal</td>
                                                        <td>Nov 21, 2021</td>
                                                        <td>$1,450</td>
                                                        <td><span className="badge bg-warning">Waiting</span></td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Monkey Karry</td>
                                                        <td>Nov 24, 2021</td>
                                                        <td>$3,500</td>
                                                        <td><span className="badge bg-success">Confirmed</span></td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">04</td>
                                                        <td>Aaron James</td>
                                                        <td>Nov 25, 2021</td>
                                                        <td>$6,875</td>
                                                        <td><span className="badge bg-danger">Cancelled</span></td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <StrippedRow />
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
                                <PreviewCardHeader title="Striped Columns" />
                                <CardBody>
                                    <p className="text-muted mb-4">Use .<code>table-striped-columns</code> to add zebra-striping to any table column.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive table-card">
                                            <table className="table align-middle table-nowrap table-striped-columns mb-0">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col" style={{ width: "46px" }}>
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" value="" id="cardtableCheck" />
                                                                <label className="form-check-label" htmlFor="cardtableCheck"></label>
                                                            </div>
                                                        </th>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Name</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Total</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col" style={{ width: "150px" }}>Action</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" value="" id="cardtableCheck01" />
                                                                <label className="form-check-label" htmlFor="cardtableCheck01"></label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2110</Link></td>
                                                        <td>William Elmore</td>
                                                        <td>07 Oct, 2021</td>
                                                        <td>$24.05</td>
                                                        <td><span className="badge bg-success">Paid</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" value="" id="cardtableCheck02" />
                                                                <label className="form-check-label" htmlFor="cardtableCheck02"></label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2109</Link></td>
                                                        <td>Georgie Winters</td>
                                                        <td>07 Oct, 2021</td>
                                                        <td>$26.15</td>
                                                        <td><span className="badge bg-success">Paid</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" value="" id="cardtableCheck03" />
                                                                <label className="form-check-label" htmlFor="cardtableCheck03"></label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2108</Link></td>
                                                        <td>Whitney Meier</td>
                                                        <td>06 Oct, 2021</td>
                                                        <td>$21.25</td>
                                                        <td><span className="badge bg-danger">Refund</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" value="" id="cardtableCheck04" />
                                                                <label className="form-check-label" htmlFor="cardtableCheck04"></label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2107</Link></td>
                                                        <td>Justin Maier</td>
                                                        <td>05 Oct, 2021</td>
                                                        <td>$25.03</td>
                                                        <td><span className="badge bg-success">Paid</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <StripedColumnsTables />
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
                                <PreviewCardHeader title="Tables Colors" />
                                <CardBody>
                                    <p className="text-muted">Use <code>table-dark</code> class to set dark color table body and <code>table-success</code> class to set success color table body respectively.
                                        use <code>table-</code> class with different color variation to set required table body color.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <div className="table-responsive">
                                                    <Table className="table-dark table-striped table-nowrap mb-0">
                                                        <thead>
                                                            <tr>
                                                                <th scope="col">ID</th>
                                                                <th scope="col">Customer</th>
                                                                <th scope="col">Date</th>
                                                                <th scope="col">Invoice</th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            <tr>
                                                                <td className="fw-medium">01</td>
                                                                <td>Bobby Davis</td>
                                                                <td>Nov 14, 2021</td>
                                                                <td>$2,410</td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">02</td>
                                                                <td>Christopher Neal</td>
                                                                <td>Nov 21, 2021</td>
                                                                <td>$1,450</td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">03</td>
                                                                <td>Monkey Karry</td>
                                                                <td>Nov 24, 2021</td>
                                                                <td>$3,500</td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">04</td>
                                                                <td>Aaron James</td>
                                                                <td>Nov 25, 2021</td>
                                                                <td>$6,875</td>
                                                            </tr>
                                                        </tbody>
                                                    </Table>
                                                </div>
                                            </Col>

                                            <Col xl={6}>
                                                <div className="table-responsive mt-4 mt-xl-0">
                                                    <Table className="table-success table-striped table-nowrap align-middle mb-0">
                                                        <thead>
                                                            <tr>
                                                                <th scope="col">ID</th>
                                                                <th scope="col">Invoice</th>
                                                                <th scope="col">Amount</th>
                                                                <th scope="col">Date</th>
                                                                <th scope="col">Status</th>
                                                                <th scope="col">Action</th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            <tr>
                                                                <td className="fw-medium">01</td>
                                                                <td>Basic Plan</td>
                                                                <td>$860</td>
                                                                <td>Nov 22, 2021</td>
                                                                <td><i className="ri-checkbox-circle-line align-middle text-success"></i> Subscribed</td>
                                                                <td>
                                                                    <div className="hstack gap-3 flex-wrap">
                                                                        <Link to="#" className="link-success fs-15"><i className="ri-edit-2-line"></i></Link>
                                                                        <Link to="#" className="link-danger fs-15"><i className="ri-delete-bin-line"></i></Link>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">02</td>
                                                                <td>Premium Plan</td>
                                                                <td>$1200</td>
                                                                <td>Nov 10, 2021</td>
                                                                <td><i className="ri-close-circle-line align-middle text-danger"></i> Unsubscribed</td>
                                                                <td>
                                                                    <div className="hstack gap-3 flex-wrap">
                                                                        <Link to="#" className="link-success fs-15"><i className="ri-edit-2-line"></i></Link>
                                                                        <Link to="#" className="link-danger fs-15"><i className="ri-delete-bin-line"></i></Link>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">03</td>
                                                                <td>Basic Plan</td>
                                                                <td>$860</td>
                                                                <td>Nov 19, 2021</td>
                                                                <td><i className="ri-checkbox-circle-line align-middle text-success"></i> Subscribed</td>
                                                                <td>
                                                                    <div className="hstack gap-3 flex-wrap">
                                                                        <Link to="#" className="link-success fs-15"><i className="ri-edit-2-line"></i></Link>
                                                                        <Link to="#" className="link-danger fs-15"><i className="ri-delete-bin-line"></i></Link>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">04</td>
                                                                <td>Corporate Plan</td>
                                                                <td>$1599</td>
                                                                <td>Nov 22, 2021</td>
                                                                <td><i className="ri-checkbox-circle-line align-middle text-success"></i> Subscribed</td>
                                                                <td>
                                                                    <div className="hstack gap-3 flex-wrap">
                                                                        <Link to="#" className="link-success fs-15"><i className="ri-edit-2-line"></i></Link>
                                                                        <Link to="#" className="link-danger fs-15"><i className="ri-delete-bin-line"></i></Link>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                        </tbody>
                                                    </Table>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <TablesColors />
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
                                <PreviewCardHeader title="Hoverable Rows" />

                                <CardBody>
                                    <p className="text-muted">Use <code>table-hover</code> class to enable a hover state on table rows within a &lt;tbody&gt;.</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <div className="table-responsive">
                                                    <Table className="table-hover align-middle table-nowrap mb-0">
                                                        <thead>
                                                            <tr>
                                                                <th scope="col" style={{ "width": "25px" }}>
                                                                    <div className="form-check">
                                                                        <Input className="form-check-input" type="checkbox" id="checkAll" defaultValue="option1" />
                                                                    </div>
                                                                </th>
                                                                <th scope="col">Order ID</th>
                                                                <th scope="col">Shop</th>
                                                                <th scope="col">Customer</th>
                                                                <th scope="col">Price</th>
                                                                <th scope="col">Action</th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            <tr>
                                                                <th scope="row">
                                                                    <div className="form-check">
                                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox2" value="option1" defaultChecked />
                                                                    </div>
                                                                </th>
                                                                <td>#541254265</td>
                                                                <td>Amezon</td>
                                                                <td>Cleo Carson</td>
                                                                <td>$4,521</td>
                                                                <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                            </tr>
                                                            <tr>
                                                                <th scope="row">
                                                                    <div className="form-check">
                                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox3" defaultValue="option1" defaultChecked />
                                                                    </div>
                                                                </th>
                                                                <td>#744145235</td>
                                                                <td>Shoppers</td>
                                                                <td>Juston Eichmann</td>
                                                                <td>$7,546</td>
                                                                <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                            </tr>
                                                            <tr>
                                                                <th scope="row">
                                                                    <div className="form-check">
                                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox4" defaultValue="option1" />
                                                                    </div>
                                                                </th>
                                                                <td>#9855126598</td>
                                                                <td>Flipkart</td>
                                                                <td>Bettie Johson</td>
                                                                <td>$1,350</td>
                                                                <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                            </tr>
                                                            <tr>
                                                                <th scope="row">
                                                                    <div className="form-check">
                                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox5" defaultValue="option1" />
                                                                    </div>
                                                                </th>
                                                                <td>#847512653</td>
                                                                <td>Shoppers</td>
                                                                <td>Maritza Blanda</td>
                                                                <td>$4,521</td>
                                                                <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                            </tr>
                                                        </tbody>
                                                    </Table>
                                                </div>
                                            </Col>

                                            <Col xl={6}>
                                                <div className="table-responsive mt-4 mt-xl-0">
                                                    <Table className="table-hover table-striped align-middle table-nowrap mb-0">
                                                        <thead>
                                                            <tr>
                                                                <th scope="col">ID</th>
                                                                <th scope="col">Invoice</th>
                                                                <th scope="col">Amount</th>
                                                                <th scope="col">Date</th>
                                                                <th scope="col">Status</th>
                                                                <th scope="col">Action</th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            <tr>
                                                                <td className="fw-medium">01</td>
                                                                <td>Basic Plan</td>
                                                                <td>$860</td>
                                                                <td>Nov 22, 2021</td>
                                                                <td><i className="ri-checkbox-circle-line align-middle text-success"></i> Subscribed</td>
                                                                <td>
                                                                    <div className="form-check form-switch">
                                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck1" defaultChecked="" />
                                                                        <Label className="form-check-label ms-2" htmlFor="SwitchCheck1">Yes/No</Label>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">02</td>
                                                                <td>Premium Plan</td>
                                                                <td>$1200</td>
                                                                <td>Nov 10, 2021</td>
                                                                <td><i className="ri-close-circle-line align-middle text-danger"></i> Unsubscribed</td>
                                                                <td>
                                                                    <div className="form-check form-switch">
                                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck2" />
                                                                        <Label className="form-check-label ms-2" htmlFor="SwitchCheck2">Yes/No</Label>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">03</td>
                                                                <td>Basic Plan</td>
                                                                <td>$860</td>
                                                                <td>Nov 19, 2021</td>
                                                                <td><i className="ri-checkbox-circle-line align-middle text-success"></i> Subscribed</td>
                                                                <td>
                                                                    <div className="form-check form-switch">
                                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck3" />
                                                                        <Label className="form-check-label ms-2" htmlFor="SwitchCheck3">Yes/No</Label>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td className="fw-medium">04</td>
                                                                <td>Corporate Plan</td>
                                                                <td>$1599</td>
                                                                <td>Nov 22, 2021</td>
                                                                <td><i className="ri-checkbox-circle-line align-middle text-success"></i> Subscribed</td>
                                                                <td>
                                                                    <div className="form-check form-switch">
                                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck4" defaultChecked="" />
                                                                        <Label className="form-check-label ms-2" htmlFor="SwitchCheck4">Yes/No</Label>
                                                                    </div>
                                                                </td>
                                                            </tr>
                                                        </tbody>
                                                    </Table>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <HoverableRows />
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
                                <PreviewCardHeader title="Card Tables" />
                                <CardBody>
                                    <p className="text-muted mb-4">Use <code>table-card</code> class to show card-based table within a &lt;tbody&gt;.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive table-card">
                                            <Table className="align-middle table-nowrap mb-0">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col" style={{ "width": "46px" }}>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="cardtableCheck" />
                                                                <Label className="form-check-label" htmlFor="cardtableCheck"></Label>
                                                            </div>
                                                        </th>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Name</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Total</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col" style={{ "width": "150px" }}>Action</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="cardtableCheck01" />
                                                                <Label className="form-check-label" htmlFor="cardtableCheck01"></Label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2110</Link></td>
                                                        <td>William Elmore</td>
                                                        <td>07 Oct, 2021</td>
                                                        <td>$24.05</td>
                                                        <td><span className="badge bg-success">Paid</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="cardtableCheck02" />
                                                                <Label className="form-check-label" htmlFor="cardtableCheck02"></Label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2109</Link></td>
                                                        <td>Georgie Winters</td>
                                                        <td>07 Oct, 2021</td>
                                                        <td>$26.15</td>
                                                        <td><span className="badge bg-success">Paid</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" value="" id="cardtableCheck03" />
                                                                <Label className="form-check-label" htmlFor="cardtableCheck03"></Label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2108</Link></td>
                                                        <td>Whitney Meier</td>
                                                        <td>06 Oct, 2021</td>
                                                        <td>$21.25</td>
                                                        <td><span className="badge bg-danger">Refund</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" value="" id="cardtableCheck04" />
                                                                <label className="form-check-label" htmlFor="cardtableCheck04"></label>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="fw-medium">#VL2107</Link></td>
                                                        <td>Justin Maier</td>
                                                        <td>05 Oct, 2021</td>
                                                        <td>$25.03</td>
                                                        <td><span className="badge bg-success">Paid</span></td>
                                                        <td>
                                                            <button type="button" className="btn btn-sm btn-light">Details</button>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <CardTables />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Active Tables" />
                                <CardBody>
                                    <p className="text-muted">Use <code>table-active</code> class to highlight a table row or cell.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="align-middle table-nowrap mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col" style={{ "width": "42px" }}>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="activetableCheck" />
                                                                <Label className="form-check-label" for="activetableCheck"></Label>
                                                            </div>
                                                        </th>
                                                        <th scope="col">Order ID</th>
                                                        <th scope="col">Shop</th>
                                                        <th scope="col">Customer</th>
                                                        <th scope="col">Price</th>
                                                        <th scope="col">Action</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="activetableCheck01" defaultChecked />
                                                                <Label className="form-check-label" for="activetableCheck01"></Label>
                                                            </div>
                                                        </th>
                                                        <td>#541254265</td>
                                                        <td>Amezon</td>
                                                        <td>Cleo Carson</td>
                                                        <td>$4,521</td>
                                                        <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                    </tr>
                                                    <tr className="table-active">
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="activetableCheck02" defaultChecked />
                                                                <Label className="form-check-label" for="activetableCheck02"></Label>
                                                            </div>
                                                        </th>
                                                        <td>#744145235</td>
                                                        <td>Shoppers</td>
                                                        <td>Juston Eichmann</td>
                                                        <td>$7,546</td>
                                                        <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="activetableCheck03" />
                                                                <Label className="form-check-label" for="activetableCheck03"></Label>
                                                            </div>
                                                        </th>
                                                        <td>#9855126598</td>
                                                        <td>Flipkart</td>
                                                        <td>Bettie Johson</td>
                                                        <td>$1,350</td>
                                                        <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="activetableCheck04" />
                                                                <Label className="form-check-label" for="activetableCheck04"></Label>
                                                            </div>
                                                        </th>
                                                        <td>#847512653</td>
                                                        <td>Shoppers</td>
                                                        <td className="table-active">Maritza Blanda</td>
                                                        <td>$4,521</td>
                                                        <td><Link to="#"><i className="ri-download-2-line fs-17 lh-1 align-middle"></i></Link></td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <ActiveTables />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Bordered Tables" />

                                <CardBody>
                                    <p className="text-muted">Use <code>table-bordered</code> class to show  borders on all sides of the table and cells.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-bordered align-middle table-nowrap mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Title</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col">Assignee</th>
                                                        <th scope="col">Price</th>
                                                        <th scope="col"></th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Implement new UX</td>
                                                        <td><span className="badge badge-soft-primary">Backlog</span></td>
                                                        <td>Lanora Sandoval</td>
                                                        <td>$4,521</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink1" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink1">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Design syntax</td>
                                                        <td><span className="badge badge-soft-secondary">In Progress</span></td>
                                                        <td>Calvin Garrett</td>
                                                        <td>$7,546</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink2" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink2">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Configurable resources</td>
                                                        <td><span className="badge badge-soft-success">Done</span></td>
                                                        <td>Florence Guzman</td>
                                                        <td>$1,350</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink3" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink3">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">04</td>
                                                        <td>Implement extensions</td>
                                                        <td><span className="badge badge-soft-secondary">In Progress</span></td>
                                                        <td>Maritza Blanda</td>
                                                        <td>$4,521</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink4" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink4">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <BorderedTables />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row >
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Tables Border Colors" />
                                <CardBody>
                                    <p className="text-muted">Use <code>border-</code> class with color variation class to set table border color.</p>

                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-bordered border-secondary table-nowrap align-middle mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Title</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col">Assignee</th>
                                                        <th scope="col">Price</th>
                                                        <th scope="col"></th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Implement new UX</td>
                                                        <td><span className="badge badge-soft-primary">Backlog</span></td>
                                                        <td>Lanora Sandoval</td>
                                                        <td>$4,521</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink5" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink5">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Design syntax</td>
                                                        <td><span className="badge badge-soft-secondary">In Progress</span></td>
                                                        <td>Calvin Garrett</td>
                                                        <td>$7,546</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink6" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink6">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Configurable resources</td>
                                                        <td><span className="badge badge-soft-success">Done</span></td>
                                                        <td>Florence Guzman</td>
                                                        <td>$1,350</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink7" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink7">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">04</td>
                                                        <td>Implement extensions</td>
                                                        <td><span className="badge badge-soft-secondary">In Progress</span></td>
                                                        <td>Maritza Blanda</td>
                                                        <td>$4,521</td>
                                                        <td>
                                                            <div className="dropdown">
                                                                <Link to="#" role="button" id="dropdownMenuLink8" data-bs-toggle="dropdown" aria-expanded="false">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </Link>

                                                                <ul className="dropdown-menu" aria-labelledby="dropdownMenuLink8">
                                                                    <li><Link className="dropdown-item" to="#">View</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Edit</Link></li>
                                                                    <li><Link className="dropdown-item" to="#">Delete</Link></li>
                                                                </ul>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <TablesBorderColors />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card >
                                <PreviewCardHeader title="Tables Without Borders" />
                                <CardBody>
                                    <p className="text-muted">Use <code>table-borderless</code> to set a table without borders.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-borderless align-middle table-nowrap mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Name</th>
                                                        <th scope="col">Job Title</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col"></th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Annette Black</td>
                                                        <td>Industrial Designer</td>
                                                        <td>10, Nov 2021</td>
                                                        <td><span className="badge badge-soft-success">Active</span></td>
                                                        <td>
                                                            <div className="hstack gap-3 fs-15">
                                                                <Link to="#" className="link-primary"><i className="ri-settings-4-line"></i></Link>
                                                                <Link to="#" className="link-danger"><i className="ri-delete-bin-5-line"></i></Link>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Bessie Cooper</td>
                                                        <td>Graphic Designer</td>
                                                        <td>13, Nov 2021</td>
                                                        <td><span className="badge badge-soft-success">Active</span></td>
                                                        <td>
                                                            <div className="hstack gap-3 fs-15">
                                                                <Link to="#" className="link-primary"><i className="ri-settings-4-line"></i></Link>
                                                                <Link to="#" className="link-danger"><i className="ri-delete-bin-5-line"></i></Link>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Leslie Alexander</td>
                                                        <td>Product Manager</td>
                                                        <td>17, Nov 2021</td>
                                                        <td><span className="badge badge-soft-success">Active</span></td>
                                                        <td>
                                                            <div className="hstack gap-3 fs-15">
                                                                <Link to="#" className="link-primary"><i className="ri-settings-4-line"></i></Link>
                                                                <Link to="#" className="link-danger"><i className="ri-delete-bin-5-line"></i></Link>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">04</td>
                                                        <td>Lenora Sandoval</td>
                                                        <td>Applications Engineer</td>
                                                        <td>25, Nov 2021</td>
                                                        <td><span className="badge badge-soft-danger">Disabled</span></td>
                                                        <td>
                                                            <div className="hstack gap-3 fs-15">
                                                                <Link to="#" className="link-primary"><i className="ri-settings-4-line"></i></Link>
                                                                <Link to="#" className="link-danger"><i className="ri-delete-bin-5-line"></i></Link>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <TablesWithoutBorders />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl="6">
                            <Card>
                                <PreviewCardHeader title="Small Tables" />

                                <CardBody>
                                    <p className="text-muted">Use <code>table-sm</code> class to create any table more compact by cutting all cell padding in half.</p>

                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-sm table-nowrap mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Title</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col">Assignee</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Implement new UX</td>
                                                        <td><span className="badge badge-soft-primary">Backlog</span></td>
                                                        <td>Lanora Sandoval</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Design syntax</td>
                                                        <td><span className="badge badge-soft-secondary">In Progress</span></td>
                                                        <td>Calvin Garrett</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Configurable resources</td>
                                                        <td><span className="badge badge-soft-success">Done</span></td>
                                                        <td>Florence Guzman</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">04</td>
                                                        <td>Implement extensions</td>
                                                        <td><span className="badge badge-soft-dark">Backlog</span></td>
                                                        <td>Maritza Blanda</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">05</td>
                                                        <td>Applications Engineer</td><td>
                                                            <span className="badge badge-soft-success">Done</span></td>
                                                        <td>Leslie Alexander</td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <SmallTables />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Table Head" />
                                <CardBody>
                                    <p className="text-muted">Use <code>table-light</code> or <code>table-dark</code> class to create &lt;thead&gt; appear light or dark.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="align-middle table-nowrap mb-0">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col">Student</th>
                                                        <th scope="col">Course</th>
                                                        <th scope="col">Author</th>
                                                        <th scope="col">Payment</th>
                                                        <th scope="col">Process</th>
                                                        <th scope="col">Status</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td>Milana Scot</td>
                                                        <td>UI/UX design</td>
                                                        <td>Mitchell Flores</td>
                                                        <td>$450</td>
                                                        <td>
                                                            <div className="progress progress-sm">
                                                                <div className="progress-bar bg-success" role="progressbar" style={{ "width": "100%" }} aria-valuenow="100" aria-valuemin="0" aria-valuemax="100"></div>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="link-success">Confirmed</Link></td>
                                                    </tr>
                                                    <tr>
                                                        <td>Jassica Welsh</td>
                                                        <td>3d Animation</td>
                                                        <td>Dan Evgeni</td>
                                                        <td>$860</td>
                                                        <td>
                                                            <div className="progress progress-sm">
                                                                <div className="progress-bar bg-warning" role="progressbar" style={{ "width": "65%" }} aria-valuenow="65" aria-valuemin="0" aria-valuemax="100"></div>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="link-warning">Waiting</Link></td>
                                                    </tr>
                                                    <tr>
                                                        <td>Leslie Alexander</td>
                                                        <td>Logotype Design</td>
                                                        <td>Olimpia Jordan</td>
                                                        <td>$450</td>
                                                        <td>
                                                            <div className="progress progress-sm">
                                                                <div className="progress-bar bg-warning" role="progressbar" style={{ "width": "35%" }} aria-valuenow="35" aria-valuemin="0" aria-valuemax="100"></div>
                                                            </div>
                                                        </td>
                                                        <td><Link to="#" className="link-warning">Waiting</Link></td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <TableHead />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Table Foot" />
                                <CardBody>
                                    <p className="text-muted">Add <code>&lt;tbody&gt;</code> attribute to group footer content in an HTML table.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-nowrap mb-0">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Name</th>
                                                        <th scope="col">Job Title</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Payment</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Annette Black</td>
                                                        <td>Industrial Designer</td>
                                                        <td>10, Nov 2021</td>
                                                        <td>$450</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Bessie Cooper</td>
                                                        <td>Graphic Designer</td>
                                                        <td>13, Nov 2021</td>
                                                        <td>$875</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Leslie Alexander</td>
                                                        <td>Product Manager</td>
                                                        <td>17, Nov 2021</td>
                                                        <td>$410</td>
                                                    </tr>
                                                </tbody>
                                                <tfoot className="table-light">
                                                    <tr>
                                                        <td colSpan="4">Total</td>
                                                        <td>$940</td>
                                                    </tr>
                                                </tfoot>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <TableFoot />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <div className="card">
                                <PreviewCardHeader title="Captions" />
                                <CardBody>
                                    <p className="text-muted">You can also put the <code>&lt;caption&gt;</code> attribute on the top of the table with <code>caption-top</code> class.</p>

                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="caption-top table-nowrap mb-0">
                                                <caption>List of users</caption>
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Student</th>
                                                        <th scope="col">Course</th>
                                                        <th scope="col">Author</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Milana Scot</td>
                                                        <td>UI/UX design</td>
                                                        <td>Mitchell Flores</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Jassica Welsh</td>
                                                        <td>3d Animation</td>
                                                        <td>Dan Evgeni</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Leslie Alexander</td>
                                                        <td>Logotype Design</td>
                                                        <td>Olimpia Jordan</td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <Captions />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </div>
                        </Col>
                    </Row>

                    <Row >
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Table Nesting" />

                                <CardBody>
                                    <p className="text-muted">Example of basic table nesting. Border styles, active styles, and table variants are not inherited by nested tables.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-nowrap mb-0">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Name</th>
                                                        <th scope="col">Job Title</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Payment</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td className="fw-medium">01</td>
                                                        <td>Annette Black</td>
                                                        <td>Industrial Designer</td>
                                                        <td>10, Nov 2021</td>
                                                        <td>$450</td>
                                                    </tr>
                                                    <tr>
                                                        <td colSpan="5">
                                                            <Table className="table-nowrap mb-0">
                                                                <thead className="table-light">
                                                                    <tr>
                                                                        <th scope="col">ID</th>
                                                                        <th scope="col">Student</th>
                                                                        <th scope="col">Course</th>
                                                                        <th scope="col">Author</th>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    <tr>
                                                                        <td className="fw-medium">I</td>
                                                                        <td>Milana Scot</td>
                                                                        <td>3d Animation</td>
                                                                        <td>James Black</td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td className="fw-medium">II</td>
                                                                        <td>Juston Eichmann</td>
                                                                        <td>Design syntax</td>
                                                                        <td>Olimpia Jordan</td>
                                                                    </tr>
                                                                </tbody>
                                                            </Table>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">02</td>
                                                        <td>Bessie Cooper</td>
                                                        <td>Graphic Designer</td>
                                                        <td>13, Nov 2021</td>
                                                        <td>$875</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">03</td>
                                                        <td>Leslie Alexander</td>
                                                        <td>Product Manager</td>
                                                        <td>16, Nov 2021</td>
                                                        <td>$410</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">04</td>
                                                        <td>Bettie Johson</td>
                                                        <td>Applications Engineer</td>
                                                        <td>24, Nov 2021</td>
                                                        <td>$620</td>
                                                    </tr>
                                                    <tr>
                                                        <td className="fw-medium">05</td>
                                                        <td>Monkey Karry</td>
                                                        <td>Implement new UX</td>
                                                        <td>25, Nov 2021</td>
                                                        <td>$795</td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <TableNesting />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Variants" />

                                <div className="card-body">
                                    <p className="text-muted">Use <code>table-</code> class with below-mentioned color variation class within &lt;tr&gt; to set color on table row.</p>

                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="table-nowrap mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">#</th>
                                                        <th scope="col">Name</th>
                                                        <th scope="col">Position</th>
                                                        <th scope="col">Department</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td>Default</td>
                                                        <td>Monkey Karry</td>
                                                        <td>Medical Specialist</td>
                                                        <td>Orthopedics</td>
                                                    </tr>
                                                    <tr className="table-primary">
                                                        <td>Primary</td>
                                                        <td>James White</td>
                                                        <td>Medical Assistant</td>
                                                        <td>Surgery</td>
                                                    </tr>
                                                    <tr className="table-secondary">
                                                        <td>Secondary</td>
                                                        <td>Aaron James</td>
                                                        <td>Medical Specialist</td>
                                                        <td>Neurology</td>
                                                    </tr>
                                                    <tr className="table-success">
                                                        <td>Success</td>
                                                        <td>Bettie Johson</td>
                                                        <td>Medical Specialist</td>
                                                        <td>Cardiology</td>
                                                    </tr>
                                                    <tr className="table-danger">
                                                        <td>Danger</td>
                                                        <td>Bessie Cooper</td>
                                                        <td>Medical Assistant</td>
                                                        <td>Surgery</td>
                                                    </tr>
                                                    <tr className="table-warning">
                                                        <td>Warning</td>
                                                        <td>Lenora Sandoval</td>
                                                        <td>Medical Assistant</td>
                                                        <td>Cardiology</td>
                                                    </tr>
                                                    <tr className="table-info">
                                                        <td>Info</td>
                                                        <td>Eric Pierce</td>
                                                        <td>Medical Specialist</td>
                                                        <td>Neurology</td>
                                                    </tr>
                                                    <tr className="table-light">
                                                        <td>Light</td>
                                                        <td>Ruth Zimmermann</td>
                                                        <td>Medical Specialist</td>
                                                        <td>Neurology</td>
                                                    </tr>
                                                    <tr className="table-dark">
                                                        <td>Dark</td>
                                                        <td>Leslie Alexander</td>
                                                        <td>Medical Assistant</td>
                                                        <td>Cardiology</td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <Variants />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={12}>
                            <Card>
                                <PreviewCardHeader title="Vertical Alignment" />
                                <CardBody>
                                    <p className="text-muted">Table cells of <code>&lt;thead&gt;</code> are always vertical aligned to the bottom. Table cells in  <code>&lt;tbody&gt;</code> inherit their alignment from <code>&lt;table&gt;</code> and are aligned to the the top by default. Use the vertical align classes to re-align where needed.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="align-middle">
                                                <thead>
                                                    <tr>
                                                        <th>Heading</th>
                                                        <th>Heading</th>
                                                        <th>Heading</th>
                                                        <th>Heading</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td>This cell inherits <code>vertical-align: middle;</code> from the table</td>
                                                        <td>This cell inherits <code>vertical-align: middle;</code> from the table</td>
                                                        <td>This cell inherits <code>vertical-align: middle;</code> from the table</td>
                                                        <td>This here is some placeholder text, intended to take up quite a bit of vertical space, to demonstrate how the vertical alignment works in the preceding cells.  Use the vertical align classes to re-align where needed. inherit their alignment from and are aligned to the the top by default.</td>
                                                    </tr>
                                                    <tr className="align-bottom">
                                                        <td>This cell inherits <code>vertical-align: bottom;</code> from the table row</td>
                                                        <td>This cell inherits <code>vertical-align: bottom;</code> from the table row</td>
                                                        <td>This cell inherits <code>vertical-align: bottom;</code> from the table row</td>
                                                        <td>This here is some placeholder text, intended to take up quite a bit of vertical space, to demonstrate how the vertical alignment works in the preceding cells.  Use the vertical align classes to re-align where needed. inherit their alignment from and are aligned to the the top by default.</td>
                                                    </tr>
                                                    <tr>
                                                        <td>This cell inherits <code>vertical-align: middle;</code> from the table</td>
                                                        <td>This cell inherits <code>vertical-align: middle;</code> from the table</td>
                                                        <td className="align-top">This cell is aligned to the top.</td>
                                                        <td>This here is some placeholder text, intended to take up quite a bit of vertical space, to demonstrate how the vertical alignment works in the preceding cells.  Use the vertical align classes to re-align where needed. inherit their alignment from and are aligned to the the top by default.</td>
                                                    </tr>
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <VerticalAlignment />
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
                                <PreviewCardHeader title="Responsive Tables" />
                                <CardBody>
                                    <p className="text-muted">Use <code>table-responsive</code> class to make any table responsive across all viewports. Responsive tables allow tables to be scrolled horizontally with ease.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <Table className="align-middle table-nowrap mb-0">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col" style={{ "width": "42px" }}>
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="responsivetableCheck" />
                                                                <Label className="form-check-label" for="responsivetableCheck"></Label>
                                                            </div>
                                                        </th>
                                                        <th scope="col">ID</th>
                                                        <th scope="col">Date</th>
                                                        <th scope="col">Status</th>
                                                        <th scope="col">Customer</th>
                                                        <th scope="col">Purchased</th>
                                                        <th scope="col">Revenue</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="responsivetableCheck01" />
                                                                <Label className="form-check-label" htmlFor="responsivetableCheck01"></Label>
                                                            </div>
                                                        </th>
                                                        <td><Link to="#w-medium">#VZ2110</Link></td>
                                                        <td>10 Oct, 14:47</td>
                                                        <td className="text-success"><i className="ri-checkbox-circle-line fs-17 align-middle"></i> Paid</td>
                                                        <td>
                                                            <div className="d-flex gap-2 align-items-center">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avtarImage3} alt="" className="avatar-xs rounded-circle" />
                                                                </div>
                                                                <div className="flex-grow-1">
                                                                    Jordan Kennedy
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td>Mastering the grid</td>
                                                        <td>$9.98</td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="responsivetableCheck02" />
                                                                <Label className="form-check-label" htmlFor="responsivetableCheck02"></Label>
                                                            </div>
                                                        </th>
                                                        <td><Link to="#" className="fw-medium">#VZ2109</Link></td>
                                                        <td>17 Oct, 02:10</td>
                                                        <td className="text-success"><i className="ri-checkbox-circle-line fs-17 align-middle"></i> Paid</td>
                                                        <td>
                                                            <div className="d-flex gap-2 align-items-center">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avtarImage4} alt="" className="avatar-xs rounded-circle" />
                                                                </div>
                                                                <div className="flex-grow-1">
                                                                    Jackson Graham
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td>Splashify</td>
                                                        <td>$270.60</td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="responsivetableCheck03" />
                                                                <Label className="form-check-label" htmlFor="responsivetableCheck03"></Label>
                                                            </div>
                                                        </th>
                                                        <td><Link to="#" className="fw-medium">#VZ2108</Link></td>
                                                        <td>26 Oct, 08:20</td>
                                                        <td className="text-primary"><i className="ri-refresh-line fs-17 align-middle"></i> Refunded</td>
                                                        <td>
                                                            <div className="d-flex gap-2 align-items-center">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avtarImage5} alt="" className="avatar-xs rounded-circle" />
                                                                </div>
                                                                <div className="flex-grow-1">
                                                                    Lauren Trujillo
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td>Wireframing Kit for Figma</td>
                                                        <td>$145.42</td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="responsivetableCheck04" />
                                                                <Label className="form-check-label" htmlFor="responsivetableCheck04"></Label>
                                                            </div>
                                                        </th>
                                                        <td><Link to="#" className="fw-medium">#VZ2107</Link></td>
                                                        <td>02 Nov, 04:52</td>
                                                        <td className="text-danger"><i className="ri-close-circle-line fs-17 align-middle"></i> Cancel</td>
                                                        <td>
                                                            <div className="d-flex gap-2 align-items-center">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avtarImage6} alt="" className="avatar-xs rounded-circle" />
                                                                </div>
                                                                <div className="flex-grow-1">
                                                                    Curtis Weaver
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td>Wireframing Kit for Figma</td>
                                                        <td>$170.68</td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <Input className="form-check-input" type="checkbox" defaultValue="" id="responsivetableCheck05" />
                                                                <Label className="form-check-label" htmlFor="responsivetableCheck05"></Label>
                                                            </div>
                                                        </th>
                                                        <td><Link to="#" className="fw-medium">#VZ2106</Link></td>
                                                        <td>10 Nov, 07:20</td>
                                                        <td className="text-success"><i className="ri-checkbox-circle-line fs-17 align-middle"></i> Paid</td>
                                                        <td>
                                                            <div className="d-flex gap-2 align-items-center">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avtarImage1} alt="" className="avatar-xs rounded-circle" />
                                                                </div>
                                                                <div className="flex-grow-1">
                                                                    Jason schuller
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td>Splashify</td>
                                                        <td>$350.87</td>
                                                    </tr>
                                                </tbody>
                                                <tfoot className="table-light">
                                                    <tr>
                                                        <td colSpan="6">Total</td>
                                                        <td>$947.55</td>
                                                    </tr>
                                                </tfoot>
                                            </Table>

                                        </div>

                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <ResponsiveTables />
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
    )
}

export default BasicTables