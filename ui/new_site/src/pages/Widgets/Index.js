import React from 'react';
import { Container } from 'reactstrap';

// import Components
import BreadCrumb from '../../Components/Common/BreadCrumb';

import TileBoxs from './TileBoxs';
import OtherWidgets from './OtherWidgets';
import UpcomingActivity from './UpcomingActivities';
import ChartMapWidgets from './Chart&MapWidgets';
import EcommerceWidgets from './EcommerceWidgets';
import CreditCard from './CreaditCard';

const Widgets = () => {
    document.title = "Widgets | Velzon - React Admin & Dashboard Template";

    return (
        <React.Fragment>
            <div className="page-content">

                <Container fluid>
                    <BreadCrumb title="Widgets" pageTitle="Velzon" />
                    {/* Tile Boxs Widgets */}
                    <TileBoxs />

                    {/* Other Widgets */}
                    <OtherWidgets />

                    {/* Upcoming Activity */}
                    <UpcomingActivity />

                    {/* Chart & Map Widgets */}
                    <ChartMapWidgets />

                    {/* EcommerceWidgets  */}
                    <EcommerceWidgets />

                    {/* Credit Card */}
                    <CreditCard />

                </Container>
            </div>
        </React.Fragment>
    );
};

export default Widgets;
