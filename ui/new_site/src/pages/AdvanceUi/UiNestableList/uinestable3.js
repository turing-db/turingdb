import React from 'react';
import ReactDragListView from 'react-drag-listview/lib/index.js';
import { Card, CardBody, CardHeader, Col, ListGroup, ListGroupItem } from 'reactstrap';

class Uinestable3 extends React.Component {
    constructor(props) {
        super(props);
        const data = [
            {
                id: 1, class: "nested-2", title: "css", icon: "mdi mdi-folder",
                set2: [{ id: 1, class: "nested-3", title: "style.css", icon: "mdi mdi-language-css3", iconClass: "secondary" }, { id: 2, class: "nested-3", title: "style.min.css", icon: "mdi mdi-language-css3", iconClass: "secondary" }]
            },
            {
                id: 2, class: "nested-2", title: "Images", icon: "mdi mdi-folder",
                set2: [{ id: 1, class: "nested-3", title: "logo.png", icon: "mdi mdi-image", iconClass: "info" }]
            },
            {
                id: 3, class: "nested-2", title: "js", icon: "mdi mdi-folder",
                set2: [{ id: 1, class: "nested-3", title: "app.js", icon: "mdi mdi-language-javascript", iconClass: "warning" }]
            },
            { id: 4, class: "nested-2", title: "index.html", icon: "mdi mdi-language-html5", iconClass: "danger", set1: true }
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

            <Col lg={6}>
                <Card>
                    <CardHeader>
                        <h4 className="card-title mb-0">Folder Structure Nested List</h4>
                    </CardHeader>

                    <CardBody>
                        <p className="text-muted">Use <code>nested-list nested-sortable</code> class to list-group class to set a nested list with sortable items where icons are given within list-group-item.</p>
                        <ListGroup className="col nested-list nested-sortable">
                            <ListGroupItem className="nested-1"> <i className="mdi mdi-folder fs-16 align-middle text-warning me-2"></i> assets
                                <ListGroup className="nested-list nested-sortable">
                                    <ReactDragListView {...dragProps}>

                                        {this.state.data.map((item, key) => (
                                            <React.Fragment key={key}>
                                                {item.set1 ? <ListGroupItem className={item.class}> <i className={"fs-16 align-middle text-" + item.iconClass + " me-2 " + item.icon}></i> {item.title}</ListGroupItem>
                                                    : <ListGroupItem className={item.class}> <i className={"fs-16 align-middle text-warning me-2 " + item.icon}></i> {item.title}
                                                        <ListGroup className="nested-list nested-sortable">
                                                            {item.set2.map((item, key) => (<ListGroupItem className={item.class} key={key}><i className={"fs-16 align-middle text-" + item.iconClass + " me-2 " + item.icon}></i> {item.title}</ListGroupItem>))}
                                                        </ListGroup>
                                                    </ListGroupItem>}
                                            </React.Fragment>))}
                                    </ReactDragListView>
                                </ListGroup>
                            </ListGroupItem>
                        </ListGroup>
                    </CardBody>
                </Card>
            </Col>

        );
    }
}

export default Uinestable3;