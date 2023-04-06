import React from 'react';
import { Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import AllTasks from './AllTasks';
import Widgets from './Widgets';


const TaskList = () => {
    document.title="Tasks List | Velzon - React Admin & Dashboard Template";

    return (
        <React.Fragment>
            <div className="page-content">           
                <Container fluid>
                    <BreadCrumb title="Tasks List" pageTitle="Tasks" />
                    <Row>
                        <Widgets />
                    </Row>
                    <AllTasks />
                </Container>
            </div>
        </React.Fragment>
    );
};

export default TaskList;