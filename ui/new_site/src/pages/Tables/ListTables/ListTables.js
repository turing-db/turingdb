import React, { useState, useEffect } from 'react';
import { Button, Card, CardBody, CardHeader, Col, Container, ListGroup, ListGroupItem, Modal, ModalBody, ModalFooter, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import SimpleBar from 'simplebar-react';
import { Link } from 'react-router-dom';
import List from 'list.js';
//Import Flatepicker
import Flatpickr from "react-flatpickr";

// Import Images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";

const ListTables = () => {
    document.title="Listjs | Velzon - React Admin & Dashboard Template";

    const [modal_list, setmodal_list] = useState(false);
    function tog_list() {
        setmodal_list(!modal_list);
    }

    const [modal_delete, setmodal_delete] = useState(false);
    function tog_delete() {
        setmodal_delete(!modal_delete);
    }

    useEffect(() => {

        const attroptions = {
            valueNames: [
                'name',
                'born',
                {
                    data: ['id']
                },
                {
                    attr: 'src',
                    name: 'image'
                },
                {
                    attr: 'href',
                    name: 'link'
                },
                {
                    attr: 'data-timestamp',
                    name: 'timestamp'
                }
            ]
        };
        const attrList = new List('users', attroptions);
        attrList.add({
            name: 'Leia',
            born: '1954',
            image: avatar5,
            id: 5,
            timestamp: '67893'
        });

        // Existing List

        const existOptionsList = {
            valueNames: ['contact-name', 'contact-message']
        };

        new List('contact-existing-list', existOptionsList);

        // Fuzzy Search list
        new List('fuzzysearch-list', {
            valueNames: ['name']
        });

        // pagination list

        new List('pagination-list', {
            valueNames: ['pagi-list'],
            page: 3,
            pagination: true
        });
    });

    return (
        <React.Fragment>
            <div className="page-content">
               
                <Container fluid>
                    <BreadCrumb title="Listjs" pageTitle="Tables" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Add, Edit & Remove</h4>
                                </CardHeader>

                                <CardBody>
                                    <div className="listjs-table" id="customerList">
                                        <Row className="g-4 mb-3">
                                            <Col className="col-sm-auto">
                                                <div>
                                                    <Button color="success" className="add-btn" onClick={() => tog_list()} id="create-btn"><i className="ri-add-line align-bottom me-1"></i> Add</Button>
                                                    <Button className="btn btn-soft-danger"
                                                    // onClick="deleteMultiple()"
                                                    ><i className="ri-delete-bin-2-line"></i></Button>
                                                </div>
                                            </Col>
                                            <Col className="col-sm">
                                                <div className="d-flex justify-content-sm-end">
                                                    <div className="search-box ms-2">
                                                        <input type="text" className="form-control search" placeholder="Search..." />
                                                        <i className="ri-search-line search-icon"></i>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>

                                        <div className="table-responsive table-card mt-3 mb-1">
                                            <table className="table align-middle table-nowrap" id="customerTable">
                                                <thead className="table-light">
                                                    <tr>
                                                        <th scope="col" style={{ width: "50px" }}>
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" id="checkAll" value="option" />
                                                            </div>
                                                        </th>
                                                        <th className="sort" data-sort="customer_name">Customer</th>
                                                        <th className="sort" data-sort="email">Email</th>
                                                        <th className="sort" data-sort="phone">Phone</th>
                                                        <th className="sort" data-sort="date">Joining Date</th>
                                                        <th className="sort" data-sort="status">Delivery Status</th>
                                                        <th className="sort" data-sort="action">Action</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="list form-check-all">
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option1" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2101</Link></td>
                                                        <td className="customer_name">Mary Cousar</td>
                                                        <td className="email">marycousar@velzon.com</td>
                                                        <td className="phone">580-464-4694</td>
                                                        <td className="date">06 Apr, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option2" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2102</Link></td>
                                                        <td className="customer_name">Jeff Taylor</td>
                                                        <td className="email">jefftaylor@velzon.com</td>
                                                        <td className="phone">863-577-5537</td>
                                                        <td className="date">15 Feb, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option3" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2103</Link></td>
                                                        <td className="customer_name">Robert McMahon</td>
                                                        <td className="email">robertmcmahon@velzon.com</td>
                                                        <td className="phone">786-253-9927</td>
                                                        <td className="date">12 Jan, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option4" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2104</Link></td>
                                                        <td className="customer_name">Michael Morris</td>
                                                        <td className="email">michaelmorris@velzon.com</td>
                                                        <td className="phone">805-447-8398</td>
                                                        <td className="date">19 May, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-danger text-uppercase">Block</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option5" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2105</Link></td>
                                                        <td className="customer_name">Kevin Dawson</td>
                                                        <td className="email">kevindawson@velzon.com</td>
                                                        <td className="phone">213-741-4294</td>
                                                        <td className="date">14 Apr, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option6" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2106</Link></td>
                                                        <td className="customer_name">Carolyn Jones</td>
                                                        <td className="email">carolynjones@velzon.com</td>
                                                        <td className="phone">414-453-5725</td>
                                                        <td className="date">07 Jun, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option7" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2107</Link></td>
                                                        <td className="customer_name">Glen Matney</td>
                                                        <td className="email">glenmatney@velzon.com</td>
                                                        <td className="phone">515-395-1069</td>
                                                        <td className="date">02 Nov, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option8" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#"
                                                            className="fw-medium link-primary">#VZ2108</Link></td>
                                                        <td className="customer_name">Charles Kubik</td>
                                                        <td className="email">charleskubik@velzon.com</td>
                                                        <td className="phone">231-480-8536</td>
                                                        <td className="date">25 Sep, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-danger text-uppercase">Block</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option9" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2109</Link></td>
                                                        <td className="customer_name">Herbert Stokes</td>
                                                        <td className="email">herbertstokes@velzon.com</td>
                                                        <td className="phone">312-944-1448</td>
                                                        <td className="date">20 Jul, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-danger text-uppercase">Block</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option10" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2110</Link></td>
                                                        <td className="customer_name">Timothy Smith</td>
                                                        <td className="email">timothysmith@velzon.com</td>
                                                        <td className="phone">973-277-6950</td>
                                                        <td className="date">13 Dec, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option11" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2111</Link></td>
                                                        <td className="customer_name">Johnny Evans</td>
                                                        <td className="email">johnnyevans@velzon.com</td>
                                                        <td className="phone">407-645-1767</td>
                                                        <td className="date">01 Oct, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-danger text-uppercase">Block</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">
                                                            <div className="form-check">
                                                                <input className="form-check-input" type="checkbox" name="checkAll" value="option12" />
                                                            </div>
                                                        </th>
                                                        <td className="id" style={{ display: "none" }}><Link to="#" className="fw-medium link-primary">#VZ2112</Link></td>
                                                        <td className="customer_name">Kevin Dawson</td>
                                                        <td className="email">kevindawson@velzon.com</td>
                                                        <td className="phone">213-741-4294</td>
                                                        <td className="date">14 Apr, 2021</td>
                                                        <td className="status"><span className="badge badge-soft-success text-uppercase">Active</span></td>
                                                        <td>
                                                            <div className="d-flex gap-2">
                                                                <div className="edit">
                                                                    <button className="btn btn-sm btn-success edit-item-btn"
                                                                        data-bs-toggle="modal" data-bs-target="#showModal">Edit</button>
                                                                </div>
                                                                <div className="remove">
                                                                    <button className="btn btn-sm btn-danger remove-item-btn" data-bs-toggle="modal" data-bs-target="#deleteRecordModal">Remove</button>
                                                                </div>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            <div className="noresult" style={{ display: "none" }}>
                                                <div className="text-center">
                                                    <lord-icon src="https://cdn.lordicon.com/msoeawqm.json" trigger="loop"
                                                        colors="primary:#121331,secondary:#08a88a" style={{ width: "75px", height: "75px" }}>
                                                    </lord-icon>
                                                    <h5 className="mt-2">Sorry! No Result Found</h5>
                                                    <p className="text-muted mb-0">We've searched more than 150+ Orders We did not find any
                                                        orders for you search.</p>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="d-flex justify-content-end">
                                            <div className="pagination-wrap hstack gap-2">
                                                <Link className="page-item pagination-prev disabled" to="#">
                                                    Previous
                                                </Link>
                                                <ul className="pagination listjs-pagination mb-0"></ul>
                                                <Link className="page-item pagination-next" to="#">
                                                    Next
                                                </Link>
                                            </div>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Data Attributes + Custom</h4>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">Use data attributes and other custom attributes as keys</p>
                                    <div id="users">
                                        <Row className="mb-2">
                                            <Col>
                                                <div>
                                                    <input className="search form-control" placeholder="Search" />
                                                </div>
                                            </Col>
                                            <Col className="col-auto">
                                                <button className="btn btn-light sort" data-sort="name">
                                                    Sort by name
                                                </button>
                                            </Col>
                                        </Row>

                                        <SimpleBar style={{ height: "242px" }} className="mx-n3">
                                            <ListGroup className="list mb-0" flush>
                                                <ListGroupItem data-id="1">
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link name text-dark">Jonny Stromberg</Link></h5>
                                                            <p className="born timestamp text-muted mb-0" data-timestamp="12345">1986</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar1} />
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem data-id="2">
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link name text-dark">Jonas Arnklint</Link></h5>
                                                            <p className="born timestamp text-muted mb-0" data-timestamp="23456">1985</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar2} />
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem data-id="3">
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link name text-dark">Martina Elm</Link></h5>
                                                            <p className="born timestamp text-muted mb-0" data-timestamp="34567">1986</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar3} />
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem data-id="4">
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link name text-dark">Gustaf Lindqvist</Link></h5>
                                                            <p className="born timestamp text-muted mb-0" data-timestamp="45678">1983</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar4} />
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                            </ListGroup >
                                        </SimpleBar>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Existing List</h4>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">Basic Example with Existing List</p>
                                    <div id="contact-existing-list">
                                        <Row className="mb-2">
                                            <Col>
                                                <div>
                                                    <input className="search form-control" placeholder="Search" />
                                                </div>
                                            </Col>
                                            <Col className="col-auto">
                                                <button className="btn btn-light sort" data-sort="contact-name">
                                                    Sort by name
                                                </button>
                                            </Col>
                                        </Row>

                                        <SimpleBar style={{ height: "242px" }} className="mx-n3">
                                            <ListGroup className="list mb-0" flush>
                                                <ListGroupItem data-id="01">
                                                    <div className="d-flex align-items-start">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar1} />
                                                            </div>
                                                        </div>

                                                        <div className="flex-grow-1 overflow-hidden">
                                                            <h5 className="contact-name fs-14 mb-1"><Link to="#" className="link text-dark">Jonny Stromberg</Link></h5>
                                                            <p className="contact-born text-muted mb-0">New updates for ABC Theme</p>
                                                        </div>

                                                        <div className="flex-shrink-0 ms-2">
                                                            <div className="fs-12 text-muted">06 min</div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem data-id="02">
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar2} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1 overflow-hidden">
                                                            <h5 className="contact-name fs-14 mb-1"><Link to="#" className="link text-dark">Jonas Arnklint</Link></h5>
                                                            <p className="contact-born text-muted mb-0">Bug Report - abc theme</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div className="fs-12 text-muted">12 min</div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem data-id="03">
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar3} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1">
                                                            <h5 className="contact-name fs-14 mb-1"><Link to="#" className="link text-dark">Martina Elm</Link></h5>
                                                            <p className="contact-born text-muted mb-0">Nice to meet you</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div className="fs-12 text-muted">28 min</div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem data-id="04">
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar4} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1">
                                                            <h5 className="contact-name fs-14 mb-1"><Link to="#" className="link text-dark">Gustaf Lindqvist</Link></h5>
                                                            <p className="contact-born text-muted mb-0">I've finished it! See you so</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div className="fs-12 text-muted">01 hrs</div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                            </ListGroup>
                                        </SimpleBar>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Fuzzy Search</h4>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">Example of how to use the fuzzy search plugin</p>
                                    <div id="fuzzysearch-list">
                                        <input type="text" className="fuzzy-search form-control mb-2" placeholder="Search" />

                                        <SimpleBar style={{ height: "242px" }}>
                                            <ul className="list mb-0">
                                                <li><p className="name">Guybrush Threepwood</p></li>
                                                <li><p className="name">Elaine Marley</p></li>
                                                <li><p className="name">LeChuck</p></li>
                                                <li><p className="name">Stan</p></li>
                                                <li><p className="name">Voodoo Lady</p></li>
                                                <li><p className="name">Herman Toothrot</p></li>
                                                <li><p className="name">Meathook</p></li>
                                                <li><p className="name">Carla</p></li>
                                                <li><p className="name">Otis</p></li>
                                                <li><p className="name">Rapp Scallion</p></li>
                                                <li><p className="name">Rum Rogers Sr.</p></li>
                                                <li><p className="name">Men of Low Moral Fiber</p></li>
                                                <li><p className="name">Murray</p></li>
                                                <li><p className="name">Cannibals</p></li>
                                            </ul>
                                        </SimpleBar>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Pagination</h4>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">Example of how to use the pagination plugin</p>

                                    <div className="listjs-table" id="pagination-list">
                                        <div className="mb-2">
                                            <input className="search form-control" placeholder="Search" />
                                        </div>

                                        <div className="mx-n3">
                                            <ListGroup className="list mb-0" flush>
                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center pagi-list">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar1} />
                                                            </div>
                                                        </div>

                                                        <div className="flex-grow-1 overflow-hidden">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link text-dark">Jonny Stromberg</Link></h5>
                                                            <p className="born timestamp text-muted mb-0">Front end Developer</p>
                                                        </div>

                                                        <div className="flex-shrink-0 ms-2">
                                                            <div>
                                                                <button type="button" className="btn btn-sm btn-light"><i className="ri-mail-line align-bottom"></i> Message</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center pagi-list">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar2} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1 overflow-hidden">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link text-dark">Jonas Arnklint</Link></h5>
                                                            <p className="born timestamp text-muted mb-0">Backend Developer</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div>
                                                                <button type="button" className="btn btn-sm btn-light"><i className="ri-mail-line align-bottom"></i> Message</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center pagi-list">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar3} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link text-dark">Martina Elm</Link></h5>
                                                            <p className="born timestamp text-muted mb-0">UI/UX Designer</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div>
                                                                <button type="button" className="btn btn-sm btn-light"><i className="ri-mail-line align-bottom"></i> Message</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center pagi-list">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar4} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link text-dark">Gustaf Lindqvist</Link></h5>
                                                            <p className="born timestamp text-muted mb-0">Full Stack Developer</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div>
                                                                <button type="button" className="btn btn-sm btn-light"><i className="ri-mail-line align-bottom"></i> Message</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center pagi-list">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar1} />
                                                            </div>
                                                        </div>

                                                        <div className="flex-grow-1 overflow-hidden">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link text-dark">Jonny Stromberg</Link></h5>
                                                            <p className="born timestamp text-muted mb-0">Front end Developer</p>
                                                        </div>

                                                        <div className="flex-shrink-0 ms-2">
                                                            <div>
                                                                <button type="button" className="btn btn-sm btn-light"><i className="ri-mail-line align-bottom"></i> Message</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>

                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center pagi-list">
                                                        <div className="flex-shrink-0 me-3">
                                                            <div>
                                                                <img className="avatar-xs rounded-circle" alt="" src={avatar2} />
                                                            </div>
                                                        </div>
                                                        <div className="flex-grow-1 overflow-hidden">
                                                            <h5 className="fs-14 mb-1"><Link to="#" className="link text-dark">Jonas Arnklint</Link></h5>
                                                            <p className="born timestamp text-muted mb-0">Backend Developer</p>
                                                        </div>
                                                        <div className="flex-shrink-0 ms-2">
                                                            <div>
                                                                <button type="button" className="btn btn-sm btn-light"><i className="ri-mail-line align-bottom"></i> Message</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                            </ListGroup>

                                            <div className="d-flex justify-content-center">
                                                <div className="pagination-wrap hstack gap-2">
                                                    <Link className="page-item pagination-prev disabled" to="#">
                                                        Previous
                                                    </Link>
                                                    <ul className="pagination listjs-pagination mb-0"></ul>
                                                    <Link className="page-item pagination-next" to="#">
                                                        Next
                                                    </Link>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>

            {/* Add Modal */}
            <Modal isOpen={modal_list} toggle={() => { tog_list(); }} centered >
                <div className="modal-header bg-light p-3">
                    <h5 className="modal-title" id="exampleModalLabel"> Add Customer </h5>
                    <Button type="button" onClick={() => { setmodal_list(false); }} className="btn-close" aria-label="Close" >
                    </Button>
                </div>
                <form className="tablelist-form">
                    <ModalBody>
                        <div className="mb-3" id="modal-id" style={{ display: "none" }}>
                            <label htmlFor="id-field" className="form-label">ID</label>
                            <input type="text" id="id-field" className="form-control" placeholder="ID" readOnly />
                        </div>

                        <div className="mb-3">
                            <label htmlFor="customername-field" className="form-label">Customer Name</label>
                            <input type="text" id="customername-field" className="form-control" placeholder="Enter Name" required />
                        </div>

                        <div className="mb-3">
                            <label htmlFor="email-field" className="form-label">Email</label>
                            <input type="email" id="email-field" className="form-control" placeholder="Enter Email" required />
                        </div>

                        <div className="mb-3">
                            <label htmlFor="phone-field" className="form-label">Phone</label>
                            <input type="text" id="phone-field" className="form-control" placeholder="Enter Phone no." required />
                        </div>

                        <div className="mb-3">
                            <label htmlFor="date-field" className="form-label">Joining Date</label>
                            <Flatpickr
                            className="form-control"
                            options={{
                                dateFormat: "d M, Y"
                            }}
                            placeholder="Select Date"
                        />
                        </div>

                        <div>
                            <label htmlFor="status-field" className="form-label">Status</label>
                            <select className="form-control" data-trigger name="status-field" id="status-field" >
                                <option value="">Status</option>
                                <option value="Active">Active</option>
                                <option value="Block">Block</option>
                            </select>
                        </div>
                    </ModalBody>
                    <ModalFooter>
                        <div className="hstack gap-2 justify-content-end">
                            <button type="button" className="btn btn-light" onClick={() => setmodal_list(false)}>Close</button>
                            <button type="submit" className="btn btn-success" id="add-btn">Add Customer</button>
                            <button type="button" className="btn btn-success" id="edit-btn">Update</button>
                        </div>
                    </ModalFooter>
                </form>
            </Modal>

            {/* Remove Modal */}
            <Modal isOpen={modal_delete} toggle={() => { tog_delete(); }} className="modal fade zoomIn" id="deleteRecordModal" centered >
                <div className="modal-header">
                    <Button type="button" onClick={() => setmodal_delete(false)} className="btn-close" aria-label="Close"> </Button>
                </div>
                <ModalBody>
                    <div className="mt-2 text-center">
                        <lord-icon src="https://cdn.lordicon.com/gsqxdxog.json" trigger="loop"
                            colors="primary:#f7b84b,secondary:#fa896b" style={{ width: "100px", height: "100px" }}></lord-icon>
                        <div className="mt-4 pt-2 fs-15 mx-4 mx-sm-5">
                            <h4>Are you Sure ?</h4>
                            <p className="text-muted mx-4 mb-0">Are you Sure You want to Remove this Record ?</p>
                        </div>
                    </div>
                    <div className="d-flex gap-2 justify-content-center mt-4 mb-2">
                        <button type="button" className="btn w-sm btn-light" onClick={() => setmodal_delete(false)}>Close</button>
                        <button type="button" className="btn w-sm btn-danger " id="delete-record">Yes, Delete It!</button>
                    </div>
                </ModalBody>
            </Modal>
        </React.Fragment>
    );
};

export default ListTables;
