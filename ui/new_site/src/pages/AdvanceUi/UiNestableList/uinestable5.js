import React from 'react';
import ReactDragListView from 'react-drag-listview/lib/index.js';
import { Card, CardBody, CardHeader, Col, ListGroup, ListGroupItem } from 'reactstrap';

// Import Images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../../assets/images/users/avatar-7.jpg";

class Uinestable5 extends React.Component {
    constructor(props) {
        super(props);
        const data = [
            {
                id: 1, name: "David Houser", designation: "Lead Designer", img: avatar3, set1: [
                    { id: 1, name: "Jame Baker", designation: "UI/UX Designer" }
                ]
            },
            {
                id: 2, name: "Mickie Reyes", designation: "Frontend Developer", img: avatar4, set1: [
                    { id: 1, name: "Thomas Perez", designation: "Developer" },
                    { id: 2, name: "Kim Wilson", designation: "Developer", img: avatar5 }
                ]
            },
            {
                id: 3, name: "Galen Coble", designation: "Backend Developer", img: avatar6, set1: [
                    { id: 1, name: "Jean Wood", designation: "Developer", img: avatar7 }
                ]
            },
        ];

        this.state = {
            data
        };
    }

    render() {
        const that = this;
        const dragProps = {
            onDragEnd(fromIndex, toIndex) {
                const data = [...that.state.data];
                const item = data.splice(fromIndex, 1)[0];
                data.splice(toIndex, 0, item);
                that.setState({ data });
            },
            nodeSelector: 'li',
            handleSelector: 'li'
        };

        return (
            <React.Fragment>
                <Col lg={6}>
                    <Card>
                        <CardHeader>
                            <h4 className="card-title mb-0">Team Nested List</h4>
                        </CardHeader>
                        <CardBody>
                            <p className="text-muted">Use <code>nested-list nested-sortable</code> class to list-group class to set a nested list with sortable items where images are attached within a list-group-item div element.</p>
                            <ListGroup className="nested-list nested-sortable">
                                <ListGroupItem className="nested-1">
                                    <div className="d-flex align-items-center">
                                        <div className="avatar-xs flex-shrink-0 me-3">
                                            <img src={avatar1} alt="" className="img-fluid rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1">
                                            <h5 className="fs-14 mb-0">Daryl Wolcott</h5>
                                            <p className="text-muted mb-0">CEO</p>
                                        </div>
                                    </div>
                                    <ListGroup className="nested-list nested-sortable">
                                        <ListGroupItem className="nested-2">
                                            <div className="d-flex align-items-center">
                                                <div className="avatar-xs flex-shrink-0 me-3">
                                                    <img src={avatar2} alt="" className="img-fluid rounded-circle" />
                                                </div>
                                                <div className="flex-grow-1">
                                                    <h5 className="fs-14 mb-0">Jacob Stanfield</h5>
                                                    <p className="text-muted mb-0">Project Manager</p>
                                                </div>
                                            </div>
                                            <ReactDragListView {...dragProps}>
                                                {this.state.data.map((item, key) => (
                                                    <React.Fragment key={key}>
                                                        <ListGroup className="nested-list nested-sortable" key={key}>
                                                            <ListGroupItem className="nested-3">
                                                                <div className="d-flex align-items-center">
                                                                    <div className="avatar-xs flex-shrink-0 me-3">
                                                                        <img src={item.img} alt="" className="img-fluid rounded-circle" />
                                                                    </div>
                                                                    <div className="flex-grow-1">
                                                                        <h5 className="fs-14 mb-0">{item.name}</h5>
                                                                        <p className="text-muted mb-0">{item.designation}</p>
                                                                    </div>
                                                                </div>
                                                                <ListGroup className="nested-list nested-sortable">
                                                                    {item.set1.map((item, key) => (<ListGroupItem className="nested-4" key={key}>
                                                                        <div className="d-flex align-items-center">
                                                                            <div className="avatar-xs flex-shrink-0 me-3">
                                                                                <div className="avatar-title rounded-circle">
                                                                                    {item.img ? <img src={item.img} alt="" className="img-fluid rounded-circle" /> : <i className="ri-user-2-fill"></i>}
                                                                                </div>
                                                                            </div>
                                                                            <div className="flex-grow-1">
                                                                                <h5 className="fs-14 mb-0">{item.name}</h5>
                                                                                <p className="text-muted mb-0">{item.designation}</p>
                                                                            </div>
                                                                        </div>
                                                                    </ListGroupItem>))}
                                                                </ListGroup>
                                                            </ListGroupItem>
                                                        </ListGroup>
                                                    </React.Fragment>
                                                ))}
                                            </ReactDragListView>
                                        </ListGroupItem>
                                    </ListGroup>
                                </ListGroupItem>
                            </ListGroup>
                        </CardBody>
                    </Card>
                </Col>
            </React.Fragment>

        );
    }
}

export default Uinestable5;