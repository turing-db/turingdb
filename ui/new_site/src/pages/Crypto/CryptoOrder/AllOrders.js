import React, { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, CardHeader, Col } from 'reactstrap';
import TableContainer from '../../../Components/Common/TableContainer';
import {
    Type,
    Quantity,
    OrderValue,
    AvgPrice,
    Price,
    Status
} from './OrderCol';

const AllOrders = ({ orderList }) => {
    const columns = useMemo(
        () => [
            {
                Header: "Date",
                Cell: (order) => (
                    <>
                        {order.row.original.date}{" "}
                        <small className="text-muted">{order.row.original.time}</small>
                    </>
                ),
            },
            {
                Header: "Name",
                Cell: (order) => (
                    <>
                        <div className="d-flex align-items-center">
                            <div className="flex-shrink-0">
                                <img src={order.row.original.img} alt="" className="avatar-xxs" />
                            </div>
                            <Link to="#" className="currency_name flex-grow-1 ms-2">{order.row.original.coinName}</Link>
                        </div>
                    </>
                ),
            },
            {
                Header: "Type",
                accessor: "type",
                filterable: false,
                Cell: (cellProps) => {
                    return <Type {...cellProps} />;
                },
            },
            {
                Header: "Quantity",
                accessor: "quantity",
                filterable: false,
                Cell: (cellProps) => {
                    return <Quantity {...cellProps} />;
                },
            },
            {
                Header: "Order Value",
                accessor: "orderValue",
                filterable: false,
                Cell: (cellProps) => {
                    return <OrderValue {...cellProps} />;
                },
            },
            {
                Header: "Avg Price",
                accessor: "avgPrice",
                filterable: false,
                Cell: (cellProps) => {
                    return <AvgPrice {...cellProps} />;
                },
            },
            {
                Header: "Price",
                accessor: "price",
                filterable: false,
                Cell: (cellProps) => {
                    return <Price {...cellProps} />;
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
        ],
        []
    );
    return (
        <React.Fragment>
            <Col lg={12}>
                <Card>
                    <CardHeader className="d-flex align-items-center border-0">
                        <h5 className="card-title mb-0 flex-grow-1">All Orders</h5>
                        <div className="flex-shrink-0">
                            <div className="flax-shrink-0 hstack gap-2">
                                <button className="btn btn-primary">Today's Orders</button>
                                <button className="btn btn-soft-info">Past Orders</button>
                            </div>
                        </div>
                    </CardHeader>
                    <CardBody>
                        <TableContainer
                            columns={columns}
                            data={(orderList || [])}
                            isGlobalFilter={true}
                            isAddUserList={false}
                            customPageSize={8}
                            className="custom-header-css"
                            divClass="table-responsive table-card mb-1"
                            tableClass="align-middle table-nowrap"
                            theadClass="table-light text-muted"
                            isCryptoOrdersFilter={true}
                            SearchPlaceholder='Search to orders...'
                        />
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default AllOrders;