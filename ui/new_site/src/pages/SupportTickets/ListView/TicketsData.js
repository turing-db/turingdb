import React, { useEffect, useMemo, useState, useCallback } from 'react';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, Form, FormFeedback, Input, Label, Modal, ModalBody, ModalHeader, Row, UncontrolledDropdown } from 'reactstrap';
//redux
import { useSelector, useDispatch } from "react-redux";
import TableContainer from '../../../Components/Common/TableContainer';
import { getTicketsList, addNewTicket, updateTicket, deleteTicket } from "../../../slices/thunks";

import { TicketsId, Title, Client, AssignedTo, CreateDate, DueDate, Status, Priority } from "./TicketCol";
//Import Flatepicker
import Flatpickr from "react-flatpickr";

import { isEmpty } from "lodash";

// Formik
import * as Yup from "yup";
import { useFormik } from "formik";

import DeleteModal from "../../../Components/Common/DeleteModal";

import { toast, ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import Loader from "../../../Components/Common/Loader";

const TicketsData = () => {
    const dispatch = useDispatch();

    const { ticketsList, isTicketSuccess, error } = useSelector((state) => ({
        ticketsList: state.Tickets.ticketsList,
        isTicketSuccess: state.Tickets.isTicketSuccess,
        error: state.Tickets.error
    }));

    const [isEdit, setIsEdit] = useState(false);
    const [ticket, setTicket] = useState([]);

    // Delete Tickets
    const [deleteModal, setDeleteModal] = useState(false);
    const [deleteModalMulti, setDeleteModalMulti] = useState(false);

    const [modal, setModal] = useState(false);

    const toggle = useCallback(() => {
        if (modal) {
            setModal(false);
            setTicket(null);
        } else {
            setModal(true);
            setcreDate(dateFormat());
            setdueDate(dateFormat());
        }
    }, [modal]);

    // validation
    const validation = useFormik({
        // enableReinitialize : use this flag when initial values needs to be changed
        enableReinitialize: true,

        initialValues: {
            id: (ticket && ticket.id) || '',
            title: (ticket && ticket.title) || '',
            client: (ticket && ticket.client) || '',
            assigned: (ticket && ticket.assigned) || '',
            create: (ticket && ticket.create) || '',
            due: (ticket && ticket.due) || '',
            status: (ticket && ticket.status) || '',
            priority: (ticket && ticket.priority) || '',
        },
        validationSchema: Yup.object({
            id: Yup.string().required("Please Enter id"),
            title: Yup.string().required("Please Enter Title"),
            client: Yup.string().required("Please Enter Client Name"),
            assigned: Yup.string().required("Please Enter Assigned Name"),
            // create: Yup.string().required("Please Enter Create Date"),
            // due: Yup.string().required("Please Enter Your Due Date"),
            status: Yup.string().required("Please Enter Your Joining status"),
            priority: Yup.string().required("Please Enter Your Priority")
        }),
        onSubmit: (values) => {
            if (isEdit) {
                const updateTickets = {
                    _id: ticket ? ticket._id : 0,
                    id: values.id,
                    title: values.title,
                    client: values.client,
                    assigned: values.assigned,
                    create: credate,
                    due: duedate,
                    status: values.status,
                    priority: values.priority,
                };
                // update ticket
                dispatch(updateTicket(updateTickets));
                validation.resetForm();
            } else {

                const newTicket = {
                    _id: (Math.floor(Math.random() * (30 - 20)) + 20).toString(),
                    id: values["id"],
                    title: values["title"],
                    client: values["client"],
                    assigned: values["assigned"],
                    create: credate,
                    due: duedate,
                    status: values["status"],
                    priority: values["priority"],
                };
                // save new ticket
                dispatch(addNewTicket(newTicket));
                validation.resetForm();
            }
            toggle();
        },
    });

    // Delete Data
    const onClickDelete = (ticket) => {
        setTicket(ticket);
        setDeleteModal(true);
    };

    const handleDeleteTicket = () => {
        if (ticket) {
            dispatch(deleteTicket(ticket._id));
            setDeleteModal(false);
        }
    };

    // Update Data
    const handleTicketsClick = useCallback((arg) => {
        const ticket = arg;

        setTicket({
            _id: ticket._id,
            id: ticket.id,
            title: ticket.title,
            client: ticket.client,
            assigned: ticket.assigned,
            create: ticket.create,
            due: ticket.due,
            status: ticket.status,
            priority: ticket.priority
        });

        setIsEdit(true);
        toggle();
    }, [toggle]);

    // Get Data

    useEffect(() => {
        if (ticketsList && !ticketsList.length) {
            dispatch(getTicketsList());
        }
    }, [dispatch, ticketsList]);


    useEffect(() => {
        setTicket(ticketsList);
    }, [ticketsList]);

    useEffect(() => {
        if (!isEmpty(ticketsList)) {
            setTicket(ticketsList);
            setIsEdit(false);
        }
    }, [ticketsList]);

    // Add Data
    const handleTicketsClicks = () => {
        setTicket("");
        setIsEdit(false);
        toggle();
    };

    // Checked All
    const checkedAll = useCallback(() => {
        const checkall = document.getElementById("checkBoxAll");
        const ele = document.querySelectorAll(".ticketCheckBox");

        if (checkall.checked) {
            ele.forEach((ele) => {
                ele.checked = true;
            });
        } else {
            ele.forEach((ele) => {
                ele.checked = false;
            });
        }
        deleteCheckbox();
    }, []);

    // Delete Multiple
    const [selectedCheckBoxDelete, setSelectedCheckBoxDelete] = useState([]);
    const [isMultiDeleteButton, setIsMultiDeleteButton] = useState(false);

    const deleteMultiple = () => {
        const checkall = document.getElementById("checkBoxAll");
        selectedCheckBoxDelete.forEach((element) => {
            dispatch(deleteTicket(element.value));
            setTimeout(() => { toast.clearWaitingQueue(); }, 3000);
        });
        setIsMultiDeleteButton(false);
        checkall.checked = false;
    };

    const deleteCheckbox = () => {
        const ele = document.querySelectorAll(".ticketCheckBox:checked");
        ele.length > 0 ? setIsMultiDeleteButton(true) : setIsMultiDeleteButton(false);
        setSelectedCheckBoxDelete(ele);
    };

    const columns = useMemo(
        () => [
            {
                Header: <input type="checkbox" id="checkBoxAll" className="form-check-input" onClick={() => checkedAll()} />,
                Cell: (cellProps) => {
                    return <input type="checkbox" className="ticketCheckBox form-check-input" value={cellProps.row.original._id} onChange={() => deleteCheckbox()} />;
                },
                id: '#',
            },
            {
                Header: "ID",
                accessor: "id",
                filterable: false,
                Cell: (cellProps) => {
                    return <TicketsId {...cellProps} />;
                },
            },
            {
                Header: "Title",
                accessor: "title",
                filterable: false,
                Cell: (cellProps) => {
                    return <Title {...cellProps} />;
                },
            },
            {
                Header: "Client",
                accessor: "client",
                filterable: false,
                Cell: (cellProps) => {
                    return <Client {...cellProps} />;
                },
            },
            {
                Header: "Assigned To",
                accessor: "assigned",
                filterable: false,
                Cell: (cellProps) => {
                    return <AssignedTo {...cellProps} />;
                },
            },
            {
                Header: "Create Date",
                accessor: "create",
                filterable: false,
                Cell: (cellProps) => {
                    return <CreateDate {...cellProps} />;
                },
            },
            {
                Header: "Due Date",
                accessor: "due",
                filterable: false,
                Cell: (cellProps) => {
                    return <DueDate {...cellProps} />;
                },
            },
            {
                Header: "Status",
                accessor: "status",
                filterable: false,
                Cell: (cellProps) => {
                    return <Status {...cellProps} />;
                },
            },
            {
                Header: "Priority",
                accessor: "priority",
                filterable: false,
                Cell: (cellProps) => {
                    return <Priority {...cellProps} />;
                },
            },
            {
                Header: "Actions",
                Cell: (cellProps) => {
                    return (
                        <UncontrolledDropdown>
                            <DropdownToggle tag="a" className="btn btn-soft-secondary btn-sm">
                                <i className="ri-more-fill align-middle"></i>
                            </DropdownToggle>
                            <DropdownMenu className="dropdown-menu-end">
                                <li><DropdownItem href="/apps-tickets-details"><i className="ri-eye-fill align-bottom me-2 text-muted"></i> View</DropdownItem></li>
                                <li><DropdownItem className="edit-item-btn" href="#showModal" data-bs-toggle="modal" onClick={() => { const TicketData = cellProps.row.original; handleTicketsClick(TicketData); }}><i className="ri-pencil-fill align-bottom me-2 text-muted"></i> Edit</DropdownItem></li>
                                <li>
                                    <DropdownItem className="remove-item-btn" data-bs-toggle="modal" href="#deleteOrder"
                                        onClick={() => {
                                            const ticketData = cellProps.row.original;
                                            onClickDelete(ticketData);
                                        }}>
                                        <i className="ri-delete-bin-fill align-bottom me-2 text-muted"></i> Delete
                                    </DropdownItem>
                                </li>
                            </DropdownMenu>
                        </UncontrolledDropdown>
                    );
                },
            },
        ],
        [handleTicketsClick, checkedAll]
    );

    const dateFormat = () => {
        let d = new Date(),
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return ((d.getDate() + ' ' + months[d.getMonth()] + ', ' + d.getFullYear()).toString());
    };

    const [credate, setcreDate] = useState(dateFormat());
    const [duedate, setdueDate] = useState(dateFormat());

    const credateformate = (e) => {
        const date = e.toString().split(" ");
        const joinDate = (date[2] + " " + date[1] + ", " + date[3]).toString();
        setcreDate(joinDate);
    };

    const duedateformate = (e) => {
        const date = e.toString().split(" ");
        const joinDate = (date[2] + " " + date[1] + ", " + date[3]).toString();
        setdueDate(joinDate);
    };

    return (
        <React.Fragment>
            <Row>
                <DeleteModal
                    show={deleteModal}
                    onDeleteClick={handleDeleteTicket}
                    onCloseClick={() => setDeleteModal(false)}
                />
                <DeleteModal
                    show={deleteModalMulti}
                    onDeleteClick={() => {
                        deleteMultiple();
                        setDeleteModalMulti(false);
                    }}
                    onCloseClick={() => setDeleteModalMulti(false)}
                />
                <Col lg={12}>
                    <Card>
                        <CardHeader className="border-0">
                            <div className="d-flex align-items-center">
                                <h5 className="card-title mb-0 flex-grow-1">Tickets</h5>
                                <div className="flex-shrink-0">
                                    <div className='d-flex flex-wrap gap-2'>
                                        <button className="btn btn-danger add-btn" onClick={() => { setIsEdit(false); toggle(); }}><i className="ri-add-line align-bottom"></i> Create Tickets</button>
                                        {" "}{isMultiDeleteButton && <button className="btn btn-secondary"
                                            onClick={() => setDeleteModalMulti(true)}
                                        ><i className="ri-delete-bin-2-line"></i></button>}
                                    </div>
                                </div>
                            </div>
                        </CardHeader>
                        
                        <CardBody className='pt-0'>
                            {isTicketSuccess && ticketsList.length ? (
                                <TableContainer
                                    columns={columns}
                                    data={(ticketsList || [])}
                                    isGlobalFilter={true}
                                    isAddUserList={false}
                                    customPageSize={8}
                                    className="custom-header-css"
                                    divClass="table-responsive table-card mb-3"
                                    tableClass="align-middle table-nowrap mb-0"
                                    theadClass=""
                                    thClass=""
                                    handleTicketClick={handleTicketsClicks}
                                    isTicketsListFilter={true}
                                    SearchPlaceholder='Search for ticket details or something...'
                                />
                            ) : (<Loader error={error} />)
                            }
                            <ToastContainer closeButton={false} limit={1} />
                        </CardBody>
                    </Card>
                </Col>
            </Row>

            <Modal
                isOpen={modal}
                toggle={toggle}
                centered
                size="lg"
                className="border-0"
                modalClassName="zoomIn"
            >

                <ModalHeader toggle={toggle} className="p-3 bg-soft-info">
                    {!!isEdit ? "Edit Ticket" : "Add Ticket"}
                </ModalHeader>
                <Form className="tablelist-form" onSubmit={(e) => {
                    e.preventDefault();
                    validation.handleSubmit();
                    return false;
                }}>
                    <ModalBody>
                        <Row className="g-3">
                            <Col lg={12}>
                                <div id="modal-id">
                                    <Label htmlFor="orderId" className="form-label">ID</Label>
                                    <Input
                                        name="id"
                                        id="orderId"
                                        className="form-control"
                                        placeholder="Enter Order Id"
                                        type="text"
                                        validate={{
                                            required: { value: true },
                                        }}
                                        onChange={validation.handleChange}
                                        onBlur={validation.handleBlur}
                                        value={validation.values.id || ""}
                                        invalid={
                                            validation.touched.id && validation.errors.id ? true : false
                                        }
                                    />
                                    {validation.touched.id && validation.errors.id ? (
                                        <FormFeedback type="invalid">{validation.errors.id}</FormFeedback>
                                    ) : null}
                                </div>
                            </Col>
                            <Col lg={12}>
                                <div>
                                    <Label htmlFor="tasksTitle-field" className="form-label">Title</Label>
                                    <Input
                                        name="title"
                                        id="tasksTitle-field"
                                        className="form-control"
                                        placeholder="Enter Title"
                                        type="text"
                                        validate={{
                                            required: { value: true },
                                        }}
                                        onChange={validation.handleChange}
                                        onBlur={validation.handleBlur}
                                        value={validation.values.title || ""}
                                        invalid={
                                            validation.touched.title && validation.errors.title ? true : false
                                        }
                                    />
                                    {validation.touched.title && validation.errors.title ? (
                                        <FormFeedback type="invalid">{validation.errors.title}</FormFeedback>
                                    ) : null}
                                </div>
                            </Col>
                            <Col lg={6}>
                                <div>
                                    <Label htmlFor="client_nameName-field" className="form-label">Client Name</Label>
                                    <Input
                                        name="client"
                                        type="text"
                                        id="client_nameName-field"
                                        placeholder="Enter Client Name"
                                        onChange={validation.handleChange}
                                        onBlur={validation.handleBlur}
                                        value={validation.values.client || ""}
                                        invalid={
                                            validation.touched.client && validation.errors.client ? true : false
                                        }
                                    />
                                    {validation.touched.client && validation.errors.client ? (
                                        <FormFeedback type="invalid">{validation.errors.client}</FormFeedback>
                                    ) : null}
                                </div>
                            </Col>
                            <Col lg={6}>
                                <div>
                                    <Label htmlFor="assignedtoName-field" className="form-label">Assigned To</Label>
                                    <Input
                                        name="assigned"
                                        type="text"
                                        id="assignedtoName-field"
                                        placeholder="Enter Assigned Name"
                                        onChange={validation.handleChange}
                                        onBlur={validation.handleBlur}
                                        value={validation.values.assigned || ""}
                                        invalid={
                                            validation.touched.assigned && validation.errors.assigned ? true : false
                                        }
                                    />
                                    {validation.touched.assigned && validation.errors.assigned ? (
                                        <FormFeedback type="invalid">{validation.errors.assigned}</FormFeedback>
                                    ) : null}
                                </div>
                            </Col>
                            <Col lg={6}>
                                <Label htmlFor="date-field" className="form-label">Create Date</Label>
                                <Flatpickr
                                    name="create"
                                    id="date-field"
                                    className="form-control"
                                    placeholder="Select a date"
                                    options={{
                                        altInput: true,
                                        altFormat: "d M, Y",
                                        dateFormat: "d M, Y",
                                    }}
                                    onChange={(e) =>
                                        credateformate(e)
                                    }
                                    value={validation.values.create || ""}
                                />

                                {validation.touched.create && validation.errors.create ? (
                                    <FormFeedback type="invalid">{validation.errors.create}</FormFeedback>
                                ) : null}
                            </Col>
                            <Col lg={6}>
                                <Label htmlFor="duedate-field" className="form-label">Due Date</Label>
                                <Flatpickr
                                    name="due"
                                    id="date-field"
                                    className="form-control"
                                    placeholder="Select a date"
                                    options={{
                                        altInput: true,
                                        altFormat: "d M, Y",
                                        dateFormat: "d M, Y",
                                    }}
                                    onChange={(e) =>
                                        duedateformate(e)
                                    }
                                    value={validation.values.due || ""}
                                />
                                {validation.touched.due && validation.errors.due ? (
                                    <FormFeedback type="invalid">{validation.errors.due}</FormFeedback>
                                ) : null}
                            </Col>
                            <Col lg={6}>
                                <Label htmlFor="ticket-status" className="form-label">Status</Label>
                                <Input
                                    name="status"
                                    type="select"
                                    className="form-select"
                                    id="status-field"
                                    onChange={validation.handleChange}
                                    onBlur={validation.handleBlur}
                                    value={
                                        validation.values.status || ""
                                    }
                                >
                                    <option value="">Status</option>
                                    <option value="New">New</option>
                                    <option value="Inprogress">Inprogress</option>
                                    <option value="Closed">Closed</option>
                                    <option value="Open">Open</option>
                                </Input>
                                {validation.touched.status &&
                                    validation.errors.status ? (
                                    <FormFeedback type="invalid">
                                        {validation.errors.status}
                                    </FormFeedback>
                                ) : null}
                            </Col>
                            <Col lg={6}>
                                <Label htmlFor="priority-field" className="form-label">Priority</Label>
                                <Input
                                    name="priority"
                                    type="select"
                                    className="form-select"
                                    id="priority-field"
                                    onChange={validation.handleChange}
                                    onBlur={validation.handleBlur}
                                    value={
                                        validation.values.priority || ""
                                    }
                                >
                                    <option value="">Priority</option>
                                    <option value="High">High</option>
                                    <option value="Medium">Medium</option>
                                    <option value="Low">Low</option>
                                </Input>
                                {validation.touched.priority &&
                                    validation.errors.priority ? (
                                    <FormFeedback type="invalid">
                                        {validation.errors.priority}
                                    </FormFeedback>
                                ) : null}
                            </Col>
                        </Row>

                    </ModalBody>
                    <div className="modal-footer">
                        <div className="hstack gap-2 justify-content-end">
                            <button onClick={() => { setModal(false); }} type="button" className="btn btn-light" data-bs-dismiss="modal">Close</button>
                            <button type="submit" className="btn btn-success" id="add-btn">{!!isEdit ? "Update" : "Add Ticket"}</button>
                        </div>
                    </div>
                </Form>
            </Modal>
        </React.Fragment>
    );
};

export default TicketsData;