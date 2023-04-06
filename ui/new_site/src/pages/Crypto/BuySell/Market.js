import React, { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, CardHeader, Col, Row } from 'reactstrap';
import { market } from '../../../common/data';
import TableContainer from '../../../Components/Common/TableContainer';
import {
    Price,
    Pairs,
    HighPrice,
    LowPrice,
    MarketVolume
} from "./MarketCol";

const Market = () => {
    const columns = useMemo(
        () => [
            {
                Header: "Currency",
                Cell: (market) => (
                    <>
                        <div className="d-flex align-items-center fw-medium">
                            <img src={market.row.original.img} alt="" className="avatar-xxs me-2" />
                            <Link to="#" className="currency_name">{market.row.original.coinName}</Link>
                        </div>
                    </>
                ),
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
                Header: "Pairs",
                accessor: "pairs",
                filterable: false,
                Cell: (cellProps) => {
                    return <Pairs {...cellProps} />;
                },
            },
            {
                Header: "24 High",
                accessor: "high",
                filterable: false,
                Cell: (cellProps) => {
                    return <HighPrice {...cellProps} />;
                },
            },
            {
                Header: "24 Low",
                accessor: "low",
                filterable: false,
                Cell: (cellProps) => {
                    return <LowPrice {...cellProps} />;
                },
            },
            {
                Header: "Market Volume",
                accessor: "marketVolume",
                filterable: false,
                Cell: (cellProps) => {
                    return <MarketVolume {...cellProps} />;
                },
            },
            {
                Header: "Volume %",
                Cell: (market) => (
                    <>
                        <h6 className={"text-" + market.row.original.iconClass + " fs-13 mb-0"}>
                            <i className={market.row.original.icon + " align-middle me-1"}></i>
                            {market.row.original.percentage}
                        </h6>
                    </>
                ),
            },
            {
                Header: "Action",
                Cell: () => (
                    <>
                        <button className="btn btn-sm btn-soft-info">Trade Now</button>
                    </>
                ),
            },
        ],
        []
    );
    return (
        <React.Fragment>
            <Card>
                <CardHeader className="border-bottom-dashed">
                    <Row className="align-items-center">
                        <Col xs={3}>
                            <h5 className="card-title mb-0">Markets</h5>
                        </Col>
                        <div className="col-auto ms-auto">
                            <div className="d-flex gap-2">
                                <button className="btn btn-success"><i className="ri-equalizer-line align-bottom me-1"></i> Filters</button>
                            </div>
                        </div>
                    </Row>
                </CardHeader>
                <CardBody className="p-0 border-bottom border-bottom-dashed">
                    <div className="search-box">
                        <input type="text" className="form-control search border-0 py-3" placeholder="Search to currency..." />
                        <i className="ri-search-line search-icon"></i>
                    </div>
                </CardBody>
                <CardBody>
                    <TableContainer
                        columns={columns}
                        data={(market || [])}
                        isGlobalFilter={false}
                        isAddUserList={false}
                        customPageSize={8}
                        className="custom-header-css"
                        divClass="table-responsive table-card"
                        tableClass="align-middle table-nowrap"
                        theadClass="table-light text-muted"
                    />
                </CardBody>
            </Card>
        </React.Fragment>
    );
};

export default Market;