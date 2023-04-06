import React from 'react';
import { Link } from 'react-router-dom';
import { Card, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { currencies } from '../../common/data';

const MyCurrencies = () => {
    return (
        <React.Fragment>
            <Col xl={8}>
                <Card className='card-height-100'>
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">My Currencies</h4>
                        <div className="flex-shrink-0">
                            <button className="btn btn-soft-primary btn-sm">24H</button>
                        </div>
                        <div className="flex-shrink-0 ms-2">
                            <UncontrolledDropdown className="card-header-dropdown" direction='start'>
                                <DropdownToggle className="btn btn-soft-primary btn-sm" role="button" tag="a">
                                    Get Report<i className="mdi mdi-chevron-down align-middle ms-1"></i>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu dropdown-menu-end">
                                    <DropdownItem>Download Report</DropdownItem>
                                    <DropdownItem>Export</DropdownItem>
                                    <DropdownItem>Import</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>
                    <div className="card-body">
                        <div className="table-responsive table-card">
                            <table className="table table-hover table-borderless table-centered align-middle table-nowrap mb-0">
                                <thead className="text-muted bg-soft-light">
                                    <tr>
                                        <th>Coin Name</th>
                                        <th>Price</th>
                                        <th>24h Change</th>
                                        <th>Total Balance</th>
                                        <th>Total Coin</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {(currencies || []).map((item, key) => (
                                        <tr key={key}>
                                            <td>
                                                <div className="d-flex align-items-center">
                                                    <div className="me-2">
                                                        <img src={item.img} alt="" className="avatar-xxs" />
                                                    </div>
                                                    <div>
                                                        <h6 className="mb-0">{item.coinName}</h6>
                                                    </div>
                                                </div>
                                            </td>
                                            <td>${item.price}</td>
                                            <td><h6 className={"mb-0 text-" + item.iconClass}><i className={"align-middle me-1 " + item.icon}></i>{item.change}</h6></td>
                                            <td>${item.balance}</td>
                                            <td>{item.totalCoin}</td>
                                            <td><Link to="/apps-crypto-buy-sell" className="btn btn-sm btn-soft-secondary">Trade</Link></td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default MyCurrencies;