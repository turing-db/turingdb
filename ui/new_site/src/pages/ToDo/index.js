import React, { useState, useEffect, useCallback } from 'react';
import { Col, Container, DropdownItem, DropdownMenu, Form, FormFeedback, Input, Modal, ModalBody, ModalHeader, Row, UncontrolledCollapse } from 'reactstrap';
import SimpleBar from 'simplebar-react';
import Flatpickr from "react-flatpickr";
import Dragula from 'react-dragula';
import { ToastContainer } from 'react-toastify';
import { Link } from 'react-router-dom';
import taskImg from "../../assets/images/task.png"
import DeleteModal from '../../Components/Common/DeleteModal';

//redux
import { useSelector, useDispatch } from 'react-redux';

// Formik
import * as Yup from "yup";
import { useFormik } from "formik";

// Import Images
import avatar1 from "../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../assets/images/users/avatar-8.jpg";
import avatar9 from "../../assets/images/users/avatar-9.jpg";
import avatar10 from "../../assets/images/users/avatar-10.jpg";

//import action
import {
    getTodos as onGetTodos,
    updateTodo as onupdateTodo,
    deleteTodo as onDeleteTodo,
    addNewTodo as onAddNewTodo,

    getProjects as onGetProjects,
    addNewProject as onAddNewProject,
} from "../../slices/thunks";

const Status = ({ status }) => {
    switch (status) {
        case "New":
            return <span className="badge badge-soft-info text-uppercase">{status}</span>;
        case "Pending":
            return <span className="badge badge-soft-warning text-uppercase">{status}</span>;
        case "Inprogress":
            return <span className="badge badge-soft-secondary text-uppercase">{status}</span>;
        case "Completed":
            return <span className="badge badge-soft-success text-uppercase">{status}</span>;
        default:
            return <span className="badge badge-soft-success text-uppercase">{status}</span>;
    }
}

const Priority = ({ priority }) => {
    switch (priority) {
        case "High":
            return <span className="badge bg-danger text-uppercase">{priority}</span>;
        case "Medium":
            return <span className="badge bg-warning text-uppercase">{priority}</span>;
        case "Low":
            return <span className="badge bg-success text-uppercase">{priority}</span>;
        default:
            return <span className="badge bg-success text-uppercase">{priority}</span>;
    }
}

const ToDoList = () => {
    document.title = "To Do Lists | Velzon - React Admin & Dashboard Template";

    const dispatch = useDispatch();

    const { todos, projects } = useSelector((state) => ({
        todos: state.Todos.todos,
        projects: state.Todos.projects,
    }));

    const [deleteModal, setDeleteModal] = useState(false);


    const [taskList, setTaskList] = useState([]);

    // Projects
    const [modalProject, setModalProject] = useState(false);

    useEffect(() => {
        dispatch(onGetProjects());
    }, [dispatch]);

    // To do Task List
    // To dos
    const [todo, setTodo] = useState(null);
    const [modalTodo, setModalTodo] = useState(false);
    const [isEdit, setIsEdit] = useState(false);


    useEffect(() => {
        dispatch(onGetTodos());
    }, [dispatch]);

    useEffect(() => {
        setTodo(todos);
        setTaskList(todos)
    }, [todos]);

    const toggle = useCallback(() => {
        if (modalTodo) {
            setModalTodo(false);
            setTodo(null);
        } else {
            setModalTodo(true);
        }
    }, [modalTodo]);

    // Toggle Project Modal
    const toggleProject = () => {
        if (modalProject) {
            setModalProject(false);
        } else {
            setModalProject(true);
        }
    };

    // Update To do
    const handleTodoClick = useCallback((arg) => {
        const todo = arg;

        setTodo({
            id: todo.id,
            task: todo.task,
            dueDate: todo.dueDate,
            status: todo.status,
            priority: todo.priority,
        });

        setIsEdit(true);
        toggle();
    }, [toggle]);

    // Add To do
    const handleTodoClicks = () => {
        setTodo("");
        setModalTodo(!modalTodo)
        setIsEdit(false);
        toggle();
    };

    // Delete To do
    const onClickTodoDelete = (todo) => {
        setTodo(todo);
        setDeleteModal(true);
    };

    const handleDeleteTodo = () => {
        if (todo) {
            dispatch(onDeleteTodo(todo));
            setDeleteModal(false);
        }
    };

    const sortbystatus = [
        {
            options: [
                { label: "Completed", value: "Completed" },
                { label: "Inprogress", value: "Inprogress" },
                { label: "New", value: "New" },
                { label: "Pending", value: "Pending" },
            ],
        },
    ];

    const sortbypriority = [
        {
            options: [
                { label: "High", value: "High" },
                { label: "Medium", value: "Medium" },
                { label: "Low", value: "Low" },
            ],
        },
    ];

    const taskStatus = (e) => {
        if (e) {
            setTaskList(todos.filter((item) => item.status === e));
        } else {
            setTaskList(todos.filter((item) => item.status !== e));
        }
    };

    const searchList = (e) => {
        let inputVal = e.toLowerCase();

        function filterItems(arr, query) {
            return arr.filter(function (el) {
                return el.task.toLowerCase().indexOf(query.toLowerCase()) !== -1
            })
        }

        let filterData = filterItems(todos, inputVal);
        setTaskList(filterData);

        if (filterData.length === 0) {
            document.getElementById("noresult").style.display = "block";
            document.getElementById("todo-task").style.display = "none";
        } else {
            document.getElementById("noresult").style.display = "none";
            document.getElementById("todo-task").style.display = "block";
        }
    }

    const taskSort = (e) => {
        if (e) {
            setTaskList(todos.sort((a, b) => a.id - b.id))
            setTaskList(todos.sort((a, b) => {
                let x = a.task.toLowerCase();
                let y = b.task.toLowerCase();
                if (x < y) {
                    return -1;
                }
                else if (x > y) {
                    return 1;
                } else {
                    return 0;
                }
            }))
        }
    }

    const changeTaskStatus = (e) => {
        const activeTask = e.target.value;
        let activeTaskList;
        if (e.target.checked) {
            activeTaskList = taskList.map((item) => {
                if (item.id === activeTask) {
                    item.status = "Completed"
                }
                return item;
            })
        } else {
            activeTaskList = taskList.map((item) => {
                if (item.id === activeTask) {
                    item.status = "Inprogress"
                }
                return item;
            })
        }
        setTaskList(activeTaskList)
    }

    // Project validation
    // validation
    const projectValidation = useFormik({
        // enableReinitialize : use this flag when initial values needs to be changed
        enableReinitialize: true,

        initialValues: {
            title: '',
        },
        validationSchema: Yup.object({
            title: Yup.string().required("Please Enter Project Title"),
        }),
        onSubmit: (values) => {
            const newProjectData = {
                id: (Math.floor(Math.random() * (30 - 20)) + 20).toString(),
                title: values.title,
                subItem: [{ id: 1, version: "v1.0.0", iconClass: "danger" }]
            };
            // save new Project Data
            dispatch(onAddNewProject(newProjectData));
            projectValidation.resetForm();
            toggleProject();
        },
    });

    // To do Task List validation
    const validation = useFormik({
        // enableReinitialize : use this flag when initial values needs to be changed
        enableReinitialize: true,

        initialValues: {
            task: (todo && todo.task) || '',
            dueDate: (todo && todo.dueDate) || '',
            status: (todo && todo.status) || '',
            priority: (todo && todo.priority) || '',
        },
        validationSchema: Yup.object({
            task: Yup.string().required("Please Enter Task"),
            // dueDate: Yup.string().required("Please Enter Date"),
            // status: Yup.string().required("Please Enter Status"),
            // priority: Yup.string().required("Please Enter Priority"),
        }),
        onSubmit: (values) => {
            if (isEdit) {
                const updateTodo = {
                    id: todo ? todo.id : 0,
                    task: values.task,
                    dueDate: date,
                    status: values.status,
                    priority: values.priority
                };
                // save edit Folder
                dispatch(onupdateTodo(updateTodo));
                validation.resetForm();
            } else {
                const newTodo = {
                    id: (Math.floor(Math.random() * (30 - 20)) + 20).toString(),
                    task: values.task,
                    dueDate: date,
                    status: values.status,
                    priority: values.priority,
                    subItem: assigned
                };
                // save new Folder
                dispatch(onAddNewTodo(newTodo));
                validation.resetForm();
            }
            toggle()
        },
    });

    const dateFormat = () => {
        let d = new Date(),
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return ((d.getDate() + ' ' + months[d.getMonth()] + ', ' + d.getFullYear()).toString());
    };

    const [date, setDate] = useState(dateFormat());

    const dateformate = (e) => {
        const date = e.toString().split(" ");
        const joinDate = (date[2] + " " + date[1] + ", " + date[3]).toString();
        setDate(joinDate);
    };

    const assigned = [
        { id: 1, img: avatar2, name: "James Forbes" },
        { id: 2, img: avatar3, name: "John Robles" },
    ]

    const assignee = [
        { id: 1, img: avatar2, name: "James Forbes" },
        { id: 2, img: avatar3, name: "John Robles" },
        { id: 3, img: avatar4, name: "Mary Gant" },
        { id: 4, img: avatar1, name: "Curtis Saenz" },
        { id: 5, img: avatar5, name: "Virgie Price" },
        { id: 6, img: avatar10, name: "Anthony Mills" },
        { id: 7, img: avatar6, name: "Marian Angel" },
        { id: 8, img: avatar7, name: "Johnnie Walton" },
        { id: 9, img: avatar8, name: "Donna Weston" },
        { id: 10, img: avatar9, name: "Diego Norris" },
    ]

    const [droplist, setDroplist] = useState(false)

    const dragulaDecorator = (componentBackingInstance) => {
        if (componentBackingInstance) {
            let options = {};
            Dragula([componentBackingInstance], options);
        }
    };

    return (
        <React.Fragment>
            <ToastContainer closeButton={false} />
            <DeleteModal
                show={deleteModal}
                onDeleteClick={() => handleDeleteTodo()}
                onCloseClick={() => setDeleteModal(false)}
            />
            <div className="page-content">
                <Container fluid>

                    <div className="chat-wrapper d-lg-flex gap-1 mx-n4 mt-n4 p-1">
                        <div className="file-manager-sidebar">
                            <div className="p-4 d-flex flex-column h-100">
                                <div className="mb-3">
                                    <button className="btn btn-success w-100" onClick={() => setModalProject(true)}><i className="ri-add-line align-bottom"></i> Add Project</button>
                                </div>

                                <SimpleBar className="px-4 mx-n4" style={{ height: "calc(100vh - 468px)" }}>

                                    <ul className="to-do-menu list-unstyled" id="projectlist-data">
                                        {(projects || []).map((item, key) => (

                                            <li key={key}>
                                                <Link to="#" className="nav-link fs-14" id={"todos" + key}>{item.title}</Link>
                                                <UncontrolledCollapse toggler={"#todos" + key}>
                                                    <ul className="mb-0 sub-menu list-unstyled ps-3 vstack gap-2 mb-2">
                                                        {(item.subItem || []).map((item, key) => (<li key={key}>
                                                            <Link to="#"><i className={"ri-stop-mini-fill align-middle fs-15 text-" + item.iconClass}></i> {item.version}</Link>
                                                        </li>))}
                                                    </ul>
                                                </UncontrolledCollapse>
                                            </li>))}

                                    </ul>
                                </SimpleBar>


                                <div className="mt-auto text-center">
                                    <img src={taskImg} alt="Task" className="img-fluid" />
                                </div>
                            </div>
                        </div>
                        <div className="file-manager-content w-100 p-4 pb-0">
                            <Row className="mb-4">
                                <div className="col-auto order-1 d-block d-lg-none">
                                    <button type="button" className="btn btn-soft-success btn-icon btn-sm fs-16 file-menu-btn">
                                        <i className="ri-menu-2-fill align-bottom"></i>
                                    </button>
                                </div>
                                <div className="col-sm order-3 order-sm-2 mt-3 mt-sm-0">
                                    <h5 className="fw-semibold mb-0">Velzon Admin & Dashboard <span className="badge bg-primary align-bottom ms-2">v2.0.0</span></h5>
                                </div>

                                <div className="col-auto order-2 order-sm-3 ms-auto">
                                    <div className="hstack gap-2">
                                        <div className="btn-group" role="group" aria-label="Basic example">
                                            <button className="btn btn-icon fw-semibold btn-soft-danger"><i className="ri-arrow-go-back-line"></i></button>
                                            <button className="btn btn-icon fw-semibold btn-soft-success"><i className="ri-arrow-go-forward-line"></i></button>
                                        </div>
                                    </div>
                                </div>
                            </Row>
                            <div className="p-3 bg-light rounded mb-4">
                                <Row className="g-2">
                                    <Col className="col-lg-auto">
                                        <select className="form-control" name="choices-select-sortlist" id="choices-select-sortlist" onChange={(e) => taskSort(e.target.value)}>
                                            <option value="">Sort</option>
                                            <option value="By ID">By ID</option>
                                            <option value="By Name">By Name</option>
                                        </select>
                                    </Col>
                                    <Col className="col-lg-auto">
                                        <select className="form-control" name="choices-select-status" id="choices-select-status" onChange={(e) => taskStatus(e.target.value)}>
                                            <option value="">All Tasks</option>
                                            <option value="Completed">Completed</option>
                                            <option value="Inprogress">Inprogress</option>
                                            <option value="Pending">Pending</option>
                                            <option value="New">New</option>
                                        </select>
                                    </Col>
                                    <Col className="col-lg">
                                        <div className="search-box">
                                            <input type="text" id="searchTaskList" className="form-control search" placeholder="Search task name" onKeyUp={(e) => searchList(e.target.value)} />
                                            <i className="ri-search-line search-icon"></i>
                                        </div>
                                    </Col>
                                    <Col className="col-lg-auto">
                                        <button className="btn btn-primary createTask" type="button" onClick={() => handleTodoClicks()}>
                                            <i className="ri-add-fill align-bottom" /> Add Tasks
                                        </button>
                                    </Col>
                                </Row>
                            </div>

                            <div className="todo-content position-relative px-4 mx-n4" id="todo-content">

                                {!todos && <div id="elmLoader">
                                    <div className="spinner-border text-primary avatar-sm" role="status">
                                        <span className="visually-hidden">Loading...</span>
                                    </div>
                                </div>}

                                <div className="todo-task" id="todo-task">
                                    <div className="table-responsive">
                                        <table className="table align-middle position-relative table-nowrap">
                                            <thead className="table-active">
                                                <tr>
                                                    <th scope="col">Task Name</th>
                                                    <th scope="col">Assigned</th>
                                                    <th scope="col">Due Date</th>
                                                    <th scope="col">Status</th>
                                                    <th scope="col">Priority</th>
                                                    <th scope="col">Action</th>
                                                </tr>
                                            </thead>

                                            <tbody id="task-list" ref={dragulaDecorator}>
                                                {(taskList || []).map((item, key) => (<tr key={key}>
                                                    <td>
                                                        <div className="d-flex align-items-start">
                                                            <div className="flex-shrink-0 me-3">
                                                                <div className="task-handle px-1 bg-light rounded">: :
                                                                </div>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <div className="form-check">
                                                                {item.status === "Completed" ? <input className="form-check-input me-1" type="checkbox" value={item.id} id={"todo" + item.id} onChange={(e) => changeTaskStatus(e)} defaultChecked /> :
                                                                        <input className="form-check-input me-1" type="checkbox" value={item.id} id={"todo" + item.id} onChange={(e) => changeTaskStatus(e)} />}
                                                                    <label className="form-check-label" htmlFor={"todo" + item.id}>{item.task} </label>
                                                                </div>
                                                            </div>
                                                        </div>
                                                    </td>
                                                    <td>
                                                        <div className="avatar-group">
                                                            {(item.subItem || []).map((item, key) => (<Link to="#" className="avatar-group-item" key={key}>
                                                                <img src={item.img} alt="" className="rounded-circle avatar-xxs" />
                                                            </Link>))}
                                                        </div>
                                                    </td>
                                                    <td>{item.dueDate}</td>
                                                    <td>
                                                        <Status status={item.status} />
                                                    </td>
                                                    <td>
                                                        <Priority priority={item.priority} />
                                                    </td>
                                                    <td>
                                                        <div className="hstack gap-2">
                                                            <button className="btn btn-sm btn-soft-danger remove-list" onClick={() => onClickTodoDelete(item)}>
                                                                <i className="ri-delete-bin-5-fill align-bottom" />
                                                            </button>
                                                            <button className="btn btn-sm btn-soft-info edit-list" onClick={() => handleTodoClick(item)}>
                                                                <i className="ri-pencil-fill align-bottom" />
                                                            </button>
                                                        </div>
                                                    </td>
                                                </tr>))}

                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                                <div className="py-4 mt-4 text-center" id="noresult" style={{ display: "none" }}>
                                    <lord-icon src="https://cdn.lordicon.com/msoeawqm.json" trigger="loop" colors="primary:#405189,secondary:#0ab39c" style={{ width: "72px", height: "72px" }}></lord-icon>
                                    <h5 className="mt-4">Sorry! No Result Found</h5>
                                </div>
                            </div>
                        </div>
                    </div>
                </Container>
            </div>

            <Modal id="createTask" isOpen={modalTodo} toggle={toggle} modalClassName="zoomIn" centered tabIndex="-1">
                <ModalHeader toggle={toggle} className="p-3 bg-soft-success"> {!!isEdit ? "Edit Task" : "Create Task"} </ModalHeader>
                <ModalBody>
                    <div id="task-error-msg" className="alert alert-danger py-2"></div>
                    <Form id="creattask-form"
                        onSubmit={(e) => {
                            e.preventDefault();
                            validation.handleSubmit();
                            return false;
                        }}
                    >
                        <input type="hidden" id="taskid-input" className="form-control" />
                        <div className="mb-3">
                            <label htmlFor="task-title-input" className="form-label">Task Title</label>
                            <input type="text" id="task-title-input" className="form-control" placeholder="Enter task title"
                                name="task"
                                validate={{ required: { value: true }, }}
                                onChange={validation.handleChange}
                                onBlur={validation.handleBlur}
                                value={validation.values.task || ""}
                            // invalid={validation.touched.task && validation.errors.task ? true : false}
                            />
                            {validation.touched.task && validation.errors.task ? (
                                <FormFeedback type="invalid">{validation.errors.task}</FormFeedback>
                            ) : null}
                        </div>
                        <div className="mb-3 position-relative">
                            <label htmlFor="task-assign-input" className="form-label">Assigned To</label>

                            <div className="avatar-group justify-content-center" id="assignee-member"></div>
                            <div className="select-element">
                                <button className="btn btn-light w-100 d-flex justify-content-between" type="button" onClick={() => setDroplist(!droplist)}>
                                    <span>Assigned To<b id="total-assignee" className="mx-1">0</b>Members</span> <i className="mdi mdi-chevron-down"></i>
                                </button>
                                <DropdownMenu className={droplist ? "w-100 show" : "w-100"}>
                                    <SimpleBar style={{ maxHeight: "141px" }}>
                                        <ul className="list-unstyled mb-0">

                                            {assignee.map((item, key) => (<li key={key}>
                                                <DropdownItem className="d-flex align-items-center" href="#">
                                                    <div className="avatar-xxs flex-shrink-0 me-2">
                                                        <img src={item.img} alt="" className="img-fluid rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1">{item.name}</div>
                                                </DropdownItem>
                                            </li>))}

                                        </ul>
                                    </SimpleBar>
                                </DropdownMenu>
                            </div>
                        </div>
                        <Row className="g-4 mb-3">
                            <Col lg={6}>
                                <label htmlFor="task-status" className="form-label">Status</label>

                                <Input
                                    name="status"
                                    type="select"
                                    className="form-select"
                                    id="status-field"
                                    onChange={validation.handleChange}
                                    onBlur={validation.handleBlur}
                                    value={validation.values.status || ""}>
                                    {sortbystatus.map((item, key) => (
                                        <React.Fragment key={key}>
                                            {item.options.map((item, key) => (<option value={item.value} key={key}>{item.label}</option>))}
                                        </React.Fragment>
                                    ))}
                                </Input>
                                {validation.touched.status &&
                                    validation.errors.status ? (
                                    <FormFeedback type="invalid">
                                        {validation.errors.status}
                                    </FormFeedback>
                                ) : null}

                            </Col>
                            <Col lg={6}>
                                <label htmlFor="priority-field" className="form-label">Priority</label>

                                <Input
                                    name="priority"
                                    type="select"
                                    className="form-select"
                                    id="priority-field"
                                    onChange={validation.handleChange}
                                    onBlur={validation.handleBlur}
                                    value={validation.values.priority || ""}>
                                    {sortbypriority.map((item, key) => (
                                        <React.Fragment key={key}>
                                            {item.options.map((item, key) => (<option value={item.value} key={key}>{item.label}</option>))}
                                        </React.Fragment>
                                    ))}
                                </Input>
                                {validation.touched.priority &&
                                    validation.errors.priority ? (
                                    <FormFeedback type="invalid">
                                        {validation.errors.priority}
                                    </FormFeedback>
                                ) : null}
                            </Col>
                        </Row>
                        <div className="mb-4">
                            <label htmlFor="task-duedate-input" className="form-label">Due Date:</label>
                            <Flatpickr
                                name="dueDate"
                                id="date-field"
                                className="form-control"
                                placeholder="Due date"
                                options={{
                                    altInput: true,
                                    altFormat: "d M, Y",
                                    dateFormat: "d M, Y",
                                }}
                                onChange={(e) => dateformate(e)}
                                value={validation.values.dueDate || ""}
                            />
                        </div>

                        <div className="hstack gap-2 justify-content-end">
                            <button type="button" className="btn btn-ghost-success" onClick={() => setModalTodo(false)}><i className="ri-close-fill align-bottom"></i> Close</button>
                            <button type="submit" className="btn btn-primary" id="addNewTodo">{!!isEdit ? "Save" : "Add Task"}</button>
                        </div>
                    </Form>
                </ModalBody>
            </Modal >

            {/* Projects */}
            <Modal id="createProjectModal" isOpen={modalProject} toggle={() => setModalProject(!modalProject)} modalClassName="zoomIn" tabIndex="-1" centered>
                <ModalHeader toggle={() => setModalProject(!modalProject)} className="p-3 bg-soft-success" id="createProjectModalLabel">
                    Create Project
                </ModalHeader>
                <ModalBody>
                    <form className="needs-validation createProject-form" onSubmit={(e) => {
                        e.preventDefault();
                        projectValidation.handleSubmit();
                        setModalProject(false)
                        return false;
                    }}>
                        <div className="mb-4">
                            <label htmlFor="projectname-input" className="form-label">Project Name</label>
                            <Input type="text" className="form-control" id="projectname-input"
                                name='title'
                                validate={{
                                    required: { value: true },
                                }}
                                onChange={projectValidation.handleChange}
                                onBlur={projectValidation.handleBlur}
                                value={projectValidation.values.title || ""}
                                placeholder="Enter project name"
                                invalid={projectValidation.touched.title && projectValidation.errors.title ? true : false}
                            />
                            {projectValidation.touched.title && projectValidation.errors.title ? (
                                <FormFeedback type="invalid">{projectValidation.errors.title}</FormFeedback>
                            ) : null}
                            <input type="hidden" className="form-control" id="projectid-input" value="" />
                        </div>
                        <div className="hstack gap-2 justify-content-end">
                            <button type="button" className="btn btn-ghost-success" onClick={() => setModalProject(false)}><i className="ri-close-line align-bottom"></i> Close</button>
                            <button type="submit" className="btn btn-primary" id="addNewProject">Add Project</button>
                        </div>
                    </form>
                </ModalBody>
            </Modal>

        </React.Fragment >
    );
};

export default ToDoList;