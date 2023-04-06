import React from 'react';
import ReactDragListView from 'react-drag-listview/lib/index.js';
import { Card, CardBody, CardHeader, Col, ListGroup, ListGroupItem } from 'reactstrap';

class Uinestable1 extends React.Component {
    constructor(props) {
        super(props);
        const data = [
            { id: 1, class: "nested-1", title: "Item 1.1"},
            {
                id: 2, class: "nested-1", title: "Item 1.2",
                set1: [{ id: 1, class: "nested-2", title: "Item 2.1"},
                {
                    id: 2, class: "nested-2", title: "Item 2.2", set2: [
                        { id: 1, class: "nested-3", title: "Item 3.1" },
                        { id: 2, class: "nested-3", title: "Item 3.2" },
                    ]
                }]
            },
            {
                id: 3, class: "nested-1", title: "Item 1.3", set1: [
                    { id: 1, class: "nested-2", title: "Item 2.1"},
                    { id: 2, class: "nested-2", title: "Item 2.2"},
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

            <Col lg={6}>
                <Card>
                    <CardHeader>
                        <h4 className="card-title mb-0">Nested Sortables List</h4>
                    </CardHeader>

                    <CardBody>
                        <p className="text-muted">Use <code>nested-list nested-sortable</code> class to list-group class to set a nested list with sortable items.</p>

                        <ListGroup className="col nested-list nested-sortable">
                            <ReactDragListView {...dragProps}>
                                {this.state.data.map((item, index) => (
                                    <React.Fragment key={index}>
                                        {!(item.set1) ? <ListGroupItem className={item.class}>{item.title}</ListGroupItem> :
                                            <ListGroupItem className={item.class}>{item.title}
                                                <ListGroup className="nested-list nested-sortable">
                                                    {item.set1.map((item, key) => (
                                                        <React.Fragment key={key}>
                                                            {!(item.set2) ? <ListGroupItem className={item.class}>{item.title}</ListGroupItem> :
                                                                <ListGroupItem className={item.class}>{item.title}
                                                                    <ListGroup className="nested-list nested-sortable">
                                                                        {item.set2.map((item, key) => (<ListGroupItem className={item.class} key={key}>{item.title}</ListGroupItem>))}
                                                                    </ListGroup>
                                                                </ListGroupItem>
                                                            }
                                                        </React.Fragment>
                                                    ))}
                                                </ListGroup>
                                            </ListGroupItem>}
                                    </React.Fragment>
                                ))}
                            </ReactDragListView>
                        </ListGroup>
                    </CardBody>
                </Card>
            </Col>

        );
    }
}

export default Uinestable1;