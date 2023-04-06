import React from 'react';
import ReactDragListView from 'react-drag-listview/lib/index.js';
import { Card, CardBody, CardHeader, Col, ListGroup, ListGroupItem } from 'reactstrap';

class Uinestable4 extends React.Component {
    constructor(props) {
        super(props);
        const data = [
            { id: 1, title: "Dashboards", icon: "ri-dashboard-2-line", set1: [{ id: 1, title: "Analytics" }, { id: 2, title: "CRM" }] },
            { id: 2, title: "Apps", icon: "ri-apps-2-line", set1: [{ id: 1, title: "Calendar" }, { id: 2, title: "Chat" }, { id: 3, title: "Email" }] },
            { id: 3, title: "Authentication", icon: "ri-account-circle-line", set1: [{ id: 1, title: "Sign In" }, { id: 2, title: "Sign Up" }] },
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
                            <h4 className="card-title mb-0">Navigation Nested List</h4>
                        </CardHeader>
                        <CardBody>
                            <p className="text-muted">Use <code>nested-list nested-sortable</code> class to list-group class to set a nested list with sortable items where the list is shown with navigation.</p>
                            <ListGroup className="nested-list nested-sortable">
                                <ReactDragListView {...dragProps}>
                                    {this.state.data.map((item, key) => (
                                        <ListGroupItem className="nested-1" key={key}> <i className={"fs-16 align-middle text-primary me-2 " + item.icon}></i> {item.title}
                                            <ListGroup className="nested-list nested-sortable">
                                                {item.set1.map((item, key) => (<ListGroupItem className="nested-2" key={key}> {item.title} </ListGroupItem>))}
                                            </ListGroup>
                                        </ListGroupItem>))}
                                </ReactDragListView>
                            </ListGroup>
                        </CardBody>
                    </Card>
                </Col>
            </React.Fragment>
        );
    }
}

export default Uinestable4;