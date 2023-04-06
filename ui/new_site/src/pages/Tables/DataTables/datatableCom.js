import React, { useState, useEffect, useMemo } from 'react';
import DataTable from 'react-data-table-component';
import { DropdownItem, DropdownMenu, DropdownToggle, Input, UncontrolledDropdown } from 'reactstrap';
import { Link } from 'react-router-dom';

//import Images
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";
import avatar9 from "../../../assets/images/users/avatar-9.jpg";
import avatar10 from "../../../assets/images/users/avatar-10.jpg";

import btc from "../../../assets/images/svg/crypto-icons/btc.svg";
import eth from "../../../assets/images/svg/crypto-icons/eth.svg";
import ltc from "../../../assets/images/svg/crypto-icons/ltc.svg";
import xmr from "../../../assets/images/svg/crypto-icons/xmr.svg";
import ant from "../../../assets/images/svg/crypto-icons/ant.svg";
import sol from "../../../assets/images/svg/crypto-icons/sol.svg";
import fil from "../../../assets/images/svg/crypto-icons/fil.svg";
import aave from "../../../assets/images/svg/crypto-icons/aave.svg";
import ada from "../../../assets/images/svg/crypto-icons/ada.svg";

const BasicTable = () => {

    const columns = [
        {
            name: <Input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />,
            cell: () => (
                <input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />
            ),
        },
        {
            name: <span className='font-weight-bold fs-13'>SR No.</span>,
            selector: row => row.srNo,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>ID</span>,
            selector: row => row.id,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Purchase ID</span>,
            selector: row => row.purchaseId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Title</span>,
            selector: row => (<Link to="#!">{row.title}</Link>),
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>User</span>,
            selector: row => row.user,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Assigned To</span>,
            selector: row => row.assigned,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create By</span>,
            selector: row => row.createdBy,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create Date</span>,
            selector: row => row.createDate,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Status</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.status) {
                    case "Re-open":
                        return <span className="badge badge-soft-info"> {cell.status} </span>;
                    case "On-Hold":
                        return <span className="badge badge-soft-secondary"> {cell.status} </span>;
                    case "Closed":
                        return <span className="badge badge-soft-danger"> {cell.status} </span>;
                    case "Inprogress":
                        return <span className="badge badge-soft-warning"> {cell.status} </span>;
                    case "Open":
                        return <span className="badge badge-soft-primary"> {cell.status} </span>;
                    case "New":
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                    default:
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Priority</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.priority) {
                    case "High":
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                    case "Medium":
                        return <span className="badge bg-info"> {cell.priority} </span>;
                    case "Low":
                        return <span className="badge bg-success"> {cell.priority} </span>;
                    default:
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Action</span>,
            sortable: true,

            cell: () => {
                return (
                    <UncontrolledDropdown className="dropdown d-inline-block">
                        <DropdownToggle className="btn btn-soft-secondary btn-sm" tag="button">
                            <i className="ri-more-fill align-middle"></i>
                        </DropdownToggle>
                        <DropdownMenu className="dropdown-menu-end">
                            <DropdownItem href="#!"><i className="ri-eye-fill align-bottom me-2 text-muted"></i>View</DropdownItem>
                            <DropdownItem className='edit-item-btn'><i className="ri-pencil-fill align-bottom me-2 text-muted"></i>Edit</DropdownItem>
                            <DropdownItem className='remove-item-btn'> <i className="ri-delete-bin-fill align-bottom me-2 text-muted"></i> Delete </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                );
            },
        },
    ];

    const data = [
        {
            srNo: "01",
            id: "VLZ-452",
            purchaseId: "VLZ1400087402",
            title: "Post launch reminder/ post list",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "03 Oct, 2021",
            status: "Re-open",
            priority: "High",
        },
        {
            srNo: "02",
            id: "VLZ-453",
            purchaseId: "VLZ1400087425",
            title: "Additional Calendar",
            user: "Diana Kohler",
            assigned: "Admin",
            createdBy: "Mary Rucker",
            createDate: "05 Oct, 2021",
            status: "On-Hold",
            priority: "Medium",
        },
        {
            srNo: "03",
            id: "VLZ-454",
            purchaseId: "VLZ1400087438",
            title: "Make a creating an account profile",
            user: "Tonya Noble",
            assigned: "Admin",
            createdBy: "Tonya Noble",
            createDate: "27 April, 2022",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "04",
            id: "VLZ-455",
            purchaseId: "VLZ1400087748",
            title: "Apologize for shopping Error!",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "14 June, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "05",
            id: "VLZ-456",
            purchaseId: "VLZ1400087547",
            title: "Support for theme",
            user: "Donald Palmer",
            assigned: "Admin",
            createdBy: "Donald Palmer",
            createDate: "25 June, 2021",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "06",
            id: "VLZ-457",
            purchaseId: "VLZ1400087245",
            title: "Benner design for FB & Twitter",
            user: "Mary Rucker",
            assigned: "Jennifer Carter",
            createdBy: "Mary Rucker",
            createDate: "14 Aug, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "07",
            id: "VLZ-458",
            purchaseId: "VLZ1400087785",
            title: "Change email option process",
            user: "James Morris",
            assigned: "Admin",
            createdBy: "James Morris",
            createDate: "12 March, 2022",
            status: "Open",
            priority: "High",
        },
        {
            srNo: "08",
            id: "VLZ-460",
            purchaseId: "VLZ1400087745",
            title: "Support for theme",
            user: "Nathan Cole",
            assigned: "Nancy Martino",
            createdBy: "Nathan Cole",
            createDate: "28 Feb, 2022",
            status: "On-Hold",
            priority: "Low",
        },
        {
            srNo: "09",
            id: "VLZ-461",
            purchaseId: "VLZ1400087179",
            title: "Form submit issue",
            user: "Grace Coles",
            assigned: "Admin",
            createdBy: "Grace Coles",
            createDate: "07 Jan, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "10",
            id: "VLZ-462",
            purchaseId: "VLZ140008856",
            title: "Edit customer testimonial",
            user: "Freda",
            assigned: "Alexis Clarke",
            createdBy: "Freda",
            createDate: "16 Aug, 2021",
            status: "Closed",
            priority: "Medium",
        },
    ];
    return (
        <DataTable
            columns={columns}
            data={data}
            pagination
        />
    );
};

const ScrollVertical = () => {
    const columns = [
        {
            name: <span className='font-weight-bold fs-13'>ID</span>,
            selector: row => row.ScrVerId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Project</span>,
            selector: row => row.project,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Task</span>,
            selector: row => (<Link to="#!">{row.task}</Link>),
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Client Name</span>,
            selector: row => row.clientName,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Assigned To</span>,
            sortable: true,
            selector: (cell) => {
                return (
                    <React.Fragment>
                        <div className="avatar-group">
                            {cell.assigned.map((item, index) => (
                                <Link key={index} to="#" className="avatar-group-item">
                                    <img src={item} alt="" className="rounded-circle avatar-xxs" />
                                </Link>
                            ))}
                        </div>
                    </React.Fragment>
                );
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Due Date</span>,
            selector: row => row.dueDate,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Status</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.status) {
                    case "Re-open":
                        return <span className="badge badge-soft-info"> {cell.status} </span>;
                    case "On-Hold":
                        return <span className="badge badge-soft-secondary"> {cell.status} </span>;
                    case "Closed":
                        return <span className="badge badge-soft-danger"> {cell.status} </span>;
                    case "Inprogress":
                        return <span className="badge badge-soft-warning"> {cell.status} </span>;
                    case "Open":
                        return <span className="badge badge-soft-primary"> {cell.status} </span>;
                    case "New":
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                    default:
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Priority</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.priority) {
                    case "High":
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                    case "Medium":
                        return <span className="badge bg-info"> {cell.priority} </span>;
                    case "Low":
                        return <span className="badge bg-success"> {cell.priority} </span>;
                    default:
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                }
            },
        },
    ];

    const data = [
        {
            ScrVerId: "VLZ-452",
            project: "Symox v1.0.0",
            task: "Add Dynamic Contact List",
            clientName: "RH Nichols",
            assigned: [avatar3],
            dueDate: "03 Oct, 2021",
            status: "Re-open",
            priority: "High",
        },
        {
            ScrVerId: "VLZ-453",
            project: "Doot - Chat App Template",
            task: "Additional Calendar",
            clientName: "Diana Kohler",
            assigned: [avatar4, avatar5],
            dueDate: "05 Oct, 2021",
            status: "On-Hold",
            priority: "Medium",
        },
        {
            ScrVerId: "VLZ-454",
            project: "Qexal - Landing Page",
            task: "Make a creating an account profile",
            clientName: "David Nichols",
            assigned: [avatar6, avatar7, avatar8],
            dueDate: "27 April, 2022",
            status: "Closed",
            priority: "Low",
        },
        {
            ScrVerId: "VLZ-455",
            project: "Dorsin - Landing Page",
            task: "Apologize for shopping Error!",
            clientName: "Tonya Noble",
            assigned: [avatar6, avatar7],
            dueDate: "14 June, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            ScrVerId: "VLZ-456",
            project: "Minimal - v2.1.0",
            task: "Support for theme",
            clientName: "Donald Palmer",
            assigned: [avatar2],
            dueDate: "25 June, 2021",
            status: "Closed",
            priority: "Low",
        },
        {
            ScrVerId: "VLZ-457",
            project: "Dason - v1.0.0",
            task: "Benner design for FB & Twitter",
            clientName: "Jennifer Carter",
            assigned: [avatar5, avatar6, avatar7, avatar8],
            dueDate: "14 Aug, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            ScrVerId: "VLZ-458",
            project: "Velzon v1.6.0",
            task: "Add datatables",
            clientName: "James Morris",
            assigned: [avatar4, avatar5],
            dueDate: "12 March, 2022",
            status: "Open",
            priority: "High",
        },
        {
            ScrVerId: "VLZ-460",
            project: "Skote v2.0.0",
            task: "Support for theme",
            clientName: "Nancy Martino",
            assigned: [avatar3, avatar10, avatar9],
            dueDate: "28 Feb, 2022",
            status: "On-Hold",
            priority: "Low",
        },
        {
            ScrVerId: "VLZ-461",
            project: "Velzon v1.0.0",
            task: "Form submit issue",
            clientName: "Grace Coles",
            assigned: [avatar5, avatar10, avatar9],
            dueDate: "07 Jan, 2022",
            status: "New",
            priority: "High",
        },
        {
            ScrVerId: "VLZ-462",
            project: "Minimal - v2.2.0",
            task: "Edit customer testimonial",
            clientName: "Freda",
            assigned: [avatar2],
            dueDate: "16 Aug, 2021",
            status: "Closed",
            priority: "Medium",
        },
        {
            ScrVerId: "VLZ-454",
            project: "Qexal - Landing Page",
            task: "Make a creating an account profile",
            clientName: "David Nichols",
            assigned: [avatar6, avatar7, avatar8],
            dueDate: "27 April, 2022",
            status: "Closed",
            priority: "Low",
        },
        {
            ScrVerId: "VLZ-455",
            project: "Dorsin - Landing Page",
            task: "Apologize for shopping Error!",
            clientName: "Tonya Noble",
            assigned: [avatar6, avatar7],
            dueDate: "14 June, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            ScrVerId: "VLZ-456",
            project: "Minimal - v2.1.0",
            task: "Support for theme",
            clientName: "Donald Palmer",
            assigned: [avatar2],
            dueDate: "25 June, 2021",
            status: "Closed",
            priority: "Low",
        },
        {
            ScrVerId: "VLZ-457",
            project: "Dason - v1.0.0",
            task: "Benner design for FB & Twitter",
            clientName: "Jennifer Carter",
            assigned: [avatar5, avatar6, avatar7, avatar8],
            dueDate: "14 Aug, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
    ];
    return (
        <DataTable
            fixedHeader
            fixedHeaderScrollHeight="300px"
            columns={columns}
            data={data}
        />
    );
};

const ScrollHorizontal = () => {
    const columns = [
        {
            name: <Input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />,
            cell: () => (
                <input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />
            ),
        },
        {
            name: <span className='font-weight-bold fs-13'>SR No.</span>,
            selector: row => row.srNo,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>ID</span>,
            selector: row => row.id,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Purchase ID</span>,
            selector: row => row.purchaseId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Title</span>,
            selector: row => (<Link to="#!">{row.title}</Link>),
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>User</span>,
            selector: row => row.user,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Assigned To</span>,
            selector: row => row.assigned,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create By</span>,
            selector: row => row.createdBy,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create Date</span>,
            selector: row => row.createDate,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Status</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.status) {
                    case "Re-open":
                        return <span className="badge badge-soft-info"> {cell.status} </span>;
                    case "On-Hold":
                        return <span className="badge badge-soft-secondary"> {cell.status} </span>;
                    case "Closed":
                        return <span className="badge badge-soft-danger"> {cell.status} </span>;
                    case "Inprogress":
                        return <span className="badge badge-soft-warning"> {cell.status} </span>;
                    case "Open":
                        return <span className="badge badge-soft-primary"> {cell.status} </span>;
                    case "New":
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                    default:
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Priority</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.priority) {
                    case "High":
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                    case "Medium":
                        return <span className="badge bg-info"> {cell.priority} </span>;
                    case "Low":
                        return <span className="badge bg-success"> {cell.priority} </span>;
                    default:
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Action</span>,
            sortable: true,
            cell: () => {
                return (
                    <UncontrolledDropdown className="dropdown d-inline-block">
                        <DropdownToggle className="btn btn-soft-secondary btn-sm" tag="button">
                            <i className="ri-more-fill align-middle"></i>
                        </DropdownToggle>
                        <DropdownMenu className="dropdown-menu-end">
                            <DropdownItem href="#!"><i className="ri-eye-fill align-bottom me-2 text-muted"></i>View</DropdownItem>
                            <DropdownItem className='edit-item-btn'><i className="ri-pencil-fill align-bottom me-2 text-muted"></i>Edit</DropdownItem>
                            <DropdownItem className='remove-item-btn'> <i className="ri-delete-bin-fill align-bottom me-2 text-muted"></i> Delete </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                );
            },
        },
    ];

    const data = [
        {
            srNo: "01",
            id: "VLZ-452",
            purchaseId: "VLZ1400087402",
            title: "Post launch reminder/ post list",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "03 Oct, 2021",
            status: "Re-open",
            priority: "High",
        },
        {
            srNo: "02",
            id: "VLZ-453",
            purchaseId: "VLZ1400087425",
            title: "Additional Calendar",
            user: "Diana Kohler",
            assigned: "Admin",
            createdBy: "Mary Rucker",
            createDate: "05 Oct, 2021",
            status: "On-Hold",
            priority: "Medium",
        },
        {
            srNo: "03",
            id: "VLZ-454",
            purchaseId: "VLZ1400087438",
            title: "Make a creating an account profile",
            user: "Tonya Noble",
            assigned: "Admin",
            createdBy: "Tonya Noble",
            createDate: "27 April, 2022",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "04",
            id: "VLZ-455",
            purchaseId: "VLZ1400087748",
            title: "Apologize for shopping Error!",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "14 June, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "05",
            id: "VLZ-456",
            purchaseId: "VLZ1400087547",
            title: "Support for theme",
            user: "Donald Palmer",
            assigned: "Admin",
            createdBy: "Donald Palmer",
            createDate: "25 June, 2021",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "07",
            id: "VLZ-458",
            purchaseId: "VLZ1400087785",
            title: "Change email option process",
            user: "James Morris",
            assigned: "Admin",
            createdBy: "James Morris",
            createDate: "12 March, 2022",
            status: "Open",
            priority: "High",
        },
        {
            srNo: "08",
            id: "VLZ-460",
            purchaseId: "VLZ1400087745",
            title: "Support for theme",
            user: "Nathan Cole",
            assigned: "Nancy Martino",
            createdBy: "Nathan Cole",
            createDate: "28 Feb, 2022",
            status: "On-Hold",
            priority: "Low",
        },
        {
            srNo: "09",
            id: "VLZ-461",
            purchaseId: "VLZ1400087179",
            title: "Form submit issue",
            user: "Grace Coles",
            assigned: "Admin",
            createdBy: "Grace Coles",
            createDate: "07 Jan, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "10",
            id: "VLZ-462",
            purchaseId: "VLZ140008856",
            title: "Edit customer testimonial",
            user: "Freda",
            assigned: "Alexis Clarke",
            createdBy: "Freda",
            createDate: "16 Aug, 2021",
            status: "Closed",
            priority: "Medium",
        },
        {
            srNo: "11",
            id: "VLZ-463",
            purchaseId: "VLZ1400078031",
            title: "Ca i have an e-copy invoice",
            user: "Williams",
            assigned: "Admin",
            createdBy: "Williams",
            createDate: "24 Feb, 2022",
            status: "Open",
            priority: "Low",
        },
        {
            srNo: "12",
            id: "VLZ-464",
            purchaseId: "VLZ1400087416",
            title: "Brand logo design",
            user: "Richard V.",
            assigned: "Admin",
            createdBy: "Richard V.",
            createDate: "16 March, 2021",
            status: "Inprogress",
            priority: "High",
        },
        {
            srNo: "13",
            id: "VLZ-466",
            purchaseId: "VLZ1400089015",
            title: "Issue with finding information about order ?",
            user: "Olive Gunther",
            assigned: "Alexis Clarke",
            createdBy: "Schaefer",
            createDate: "32 March, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "14",
            id: "VLZ-467",
            purchaseId: "VLZ1400090324",
            title: "Make a creating an account profile",
            user: "Edwin",
            assigned: "Admin",
            createdBy: "Edwin",
            createDate: "05 April, 2022",
            status: "Inprogress",
            priority: "Low",
        },
    ];
    return (
        <DataTable
            columns={columns}
            data={data}
            pagination
        />
    );
};

const AlternativePagination = () => {
    const columns = [
        {
            name: <span className='font-weight-bold fs-13'>SR No.</span>,
            selector: row => row.srNo,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Currency</span>,
            sortable: true,
            selector: (cell) => {
                return (
                    <React.Fragment>
                        <div className="d-flex align-items-center fw-medium" >
                            <img src={cell.currency[0]} alt="" className="avatar-xxs me-2" />
                            <Link to="#" className="currency_name">{cell.currency[1]}</Link>
                        </div>
                    </React.Fragment>);
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Price</span>,
            selector: row => row.price,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Pairs</span>,
            selector: row => row.pairs,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>24 High</span>,
            selector: row => row.high,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>24 Low</span>,
            selector: row => row.low,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Market Volume</span>,
            selector: row => row.marketVolume,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Volume %</span>,
            sortable: true,
            selector: (cell) => {
                return (
                    <h6 className={"fs-13 mb-0 text-" + cell.volumeClass} > <i className={"align-middle me-1 " + cell.icon}></i>{cell.volume}</h6>
                );
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Action</span>,
            sortable: true,
            cell: () => {
                return (<button className="btn btn-sm btn-soft-info">Trade Now</button>);
            },
        },
    ];

    const data = [
        {
            srNo: "01",
            currency: [btc, "Bitcoin (BTC)"],
            price: "$47,071.60",
            pairs: "BTC/USD",
            high: "$28,722.76",
            low: "$68,789.63",
            marketVolume: "$888,411,910",
            volume: "1.50%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "02",
            currency: [eth, "Ethereum (ETH)"],
            price: "$3,813.14",
            pairs: "ETH/USDT",
            high: "$4,036.24",
            low: "$3,588.14",
            marketVolume: "$314,520,675",
            volume: "0.42%",
            icon: "mdi mdi-trending-down",
            volumeClass: "danger"
        },
        {
            srNo: "03",
            currency: [ltc, "Litecoin (LTC)"],
            price: "$149.65",
            pairs: "LTC/USDT",
            high: "$412.96",
            low: "$104.33",
            marketVolume: "$314,520,675",
            volume: "0.89%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "04",
            currency: [xmr, "Monero (XMR)"],
            price: "$17,491.16",
            pairs: "XRM/USDT",
            high: "$31,578.35",
            low: "$8691.75",
            marketVolume: "$9,847,327",
            volume: "1.92%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "05",
            currency: [ant, "Aragon (ANT)"],
            price: "$172.93",
            pairs: "SOL/USD",
            high: "$178.37",
            low: "$172.3",
            marketVolume: "$40,559,274",
            volume: "2.87%",
            icon: "mdi mdi-trending-down",
            volumeClass: "danger"
        },
        {
            srNo: "06",
            currency: [sol, "Solana (SOL)"],
            price: "$17,491.16",
            pairs: "XRM/USDT",
            high: "$31,578.35",
            low: "$8691.75",
            marketVolume: "$9,847,327",
            volume: "1.92%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "07",
            currency: [fil, "Filecoin (FIL)"],
            price: "$13.31",
            pairs: "ANT/USD",
            high: "$13.85",
            low: "$12.53",
            marketVolume: "$156,209,195.18",
            volume: "3.96%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "08",
            currency: [fil, "Filecoin (FIL)"],
            price: "$35.21",
            pairs: "FIL/USD",
            high: "$36.41",
            low: "$35.03",
            marketVolume: "$374,618,945.51",
            volume: "0.84%",
            icon: "mdi mdi-trending-down",
            volumeClass: "danger"
        },
        {
            srNo: "09",
            currency: [aave, "Aave (AAVE)"],
            price: "$275.47",
            pairs: "AAVE/USDT",
            high: "$277.11",
            low: "$255.01",
            marketVolume: "$156,209,195.18",
            volume: "8.20%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "10",
            currency: [ada, "Cardano (ADA)"],
            price: "$1.35",
            pairs: "ADA/USD",
            high: "$1.39",
            low: "$1.32",
            marketVolume: "$880,387,980.14",
            volume: "0.42%",
            icon: "mdi mdi-trending-down",
            volumeClass: "danger"
        },
        {
            srNo: "11",
            currency: [fil, "Filecoin (FIL)"],
            price: "$35.21",
            pairs: "FIL/USD",
            high: "$36.41",
            low: "$35.03",
            marketVolume: "$374,618,945.51",
            volume: "0.84%",
            icon: "mdi mdi-trending-down",
            volumeClass: "danger"
        },
        {
            srNo: "12",
            currency: [aave, "Aave (AAVE)"],
            price: "$275.47",
            pairs: "AAVE/USDT",
            high: "$277.11",
            low: "$255.01",
            marketVolume: "$156,209,195.18",
            volume: "8.20%",
            icon: "mdi mdi-trending-up",
            volumeClass: "success"
        },
        {
            srNo: "13",
            currency: [ada, "Cardano (ADA)"],
            price: "$1.35",
            pairs: "ADA/USD",
            high: "$1.39",
            low: "$1.32",
            marketVolume: "$880,387,980.14",
            volume: "0.42%",
            icon: "mdi mdi-trending-down",
            volumeClass: "danger"
        },
    ];
    return (
        <DataTable
            columns={columns}
            data={data}
            pagination
        />
    );
};

const FixedHeaderDatatables = () => {
    const columns = [
        {
            name: <Input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />,
            cell: () => (
                <input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />
            ),
        },
        {
            name: <span className='font-weight-bold fs-13'>SR No.</span>,
            selector: row => row.srNo,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>ID</span>,
            selector: row => row.FixHeadId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Purchase ID</span>,
            selector: row => row.purchaseId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Title</span>,
            selector: row => (<Link to="#!">{row.title}</Link>),
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>User</span>,
            selector: row => row.user,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Assigned To</span>,
            selector: row => row.assigned,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create By</span>,
            selector: row => row.createdBy,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create Date</span>,
            selector: row => row.createDate,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Status</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.status) {
                    case "Re-open":
                        return <span className="badge badge-soft-info"> {cell.status} </span>;
                    case "On-Hold":
                        return <span className="badge badge-soft-secondary"> {cell.status} </span>;
                    case "Closed":
                        return <span className="badge badge-soft-danger"> {cell.status} </span>;
                    case "Inprogress":
                        return <span className="badge badge-soft-warning"> {cell.status} </span>;
                    case "Open":
                        return <span className="badge badge-soft-primary"> {cell.status} </span>;
                    case "New":
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                    default:
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Priority</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.priority) {
                    case "High":
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                    case "Medium":
                        return <span className="badge bg-info"> {cell.priority} </span>;
                    case "Low":
                        return <span className="badge bg-success"> {cell.priority} </span>;
                    default:
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Action</span>,
            sortable: true,

            cell: () => {
                return (
                    <UncontrolledDropdown className="dropdown d-inline-block">
                        <DropdownToggle className="btn btn-soft-secondary btn-sm" tag="button">
                            <i className="ri-more-fill align-middle"></i>
                        </DropdownToggle>
                        <DropdownMenu className="dropdown-menu-end">
                            <DropdownItem href="#!"><i className="ri-eye-fill align-bottom me-2 text-muted"></i>View</DropdownItem>
                            <DropdownItem className='edit-item-btn'><i className="ri-pencil-fill align-bottom me-2 text-muted"></i>Edit</DropdownItem>
                            <DropdownItem className='remove-item-btn'> <i className="ri-delete-bin-fill align-bottom me-2 text-muted"></i> Delete </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                );
            },
        },
    ];

    const data = [
        {
            srNo: "01",
            FixHeadId: "VLZ-452",
            purchaseId: "VLZ1400087402",
            title: "Post launch reminder/ post list",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "03 Oct, 2021",
            status: "Re-open",
            priority: "High",
        },
        {
            srNo: "02",
            FixHeadId: "VLZ-453",
            purchaseId: "VLZ1400087425",
            title: "Additional Calendar",
            user: "Diana Kohler",
            assigned: "Admin",
            createdBy: "Mary Rucker",
            createDate: "05 Oct, 2021",
            status: "On-Hold",
            priority: "Medium",
        },
        {
            srNo: "03",
            FixHeadId: "VLZ-454",
            purchaseId: "VLZ1400087438",
            title: "Make a creating an account profile",
            user: "Tonya Noble",
            assigned: "Admin",
            createdBy: "Tonya Noble",
            createDate: "27 April, 2022",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "04",
            FixHeadId: "VLZ-455",
            purchaseId: "VLZ1400087748",
            title: "Apologize for shopping Error!",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "14 June, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "05",
            FixHeadId: "VLZ-456",
            purchaseId: "VLZ1400087547",
            title: "Support for theme",
            user: "Donald Palmer",
            assigned: "Admin",
            createdBy: "Donald Palmer",
            createDate: "25 June, 2021",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "06",
            FixHeadId: "VLZ-457",
            purchaseId: "VLZ1400087245",
            title: "Benner design for FB & Twitter",
            user: "Mary Rucker",
            assigned: "Jennifer Carter",
            createdBy: "Mary Rucker",
            createDate: "14 Aug, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "07",
            FixHeadId: "VLZ-458",
            purchaseId: "VLZ1400087785",
            title: "Change email option process",
            user: "James Morris",
            assigned: "Admin",
            createdBy: "James Morris",
            createDate: "12 March, 2022",
            status: "Open",
            priority: "High",
        },
        {
            srNo: "08",
            FixHeadId: "VLZ-460",
            purchaseId: "VLZ1400087745",
            title: "Support for theme",
            user: "Nathan Cole",
            assigned: "Nancy Martino",
            createdBy: "Nathan Cole",
            createDate: "28 Feb, 2022",
            status: "On-Hold",
            priority: "Low",
        },
        {
            srNo: "09",
            FixHeadId: "VLZ-461",
            purchaseId: "VLZ1400087179",
            title: "Form submit issue",
            user: "Grace Coles",
            assigned: "Admin",
            createdBy: "Grace Coles",
            createDate: "07 Jan, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "10",
            FixHeadId: "VLZ-462",
            purchaseId: "VLZ140008856",
            title: "Edit customer testimonial",
            user: "Freda",
            assigned: "Alexis Clarke",
            createdBy: "Freda",
            createDate: "16 Aug, 2021",
            status: "Closed",
            priority: "Medium",
        },
        {
            srNo: "11",
            FixHeadId: "VLZ-463",
            purchaseId: "VLZ1400078031",
            title: "Ca i have an e-copy invoice",
            user: "Williams",
            assigned: "Admin",
            createdBy: "Williams",
            createDate: "24 Feb, 2022",
            status: "Open",
            priority: "Low",
        },
        {
            srNo: "12",
            FixHeadId: "VLZ-464",
            purchaseId: "VLZ1400087416",
            title: "Brand logo design",
            user: "Richard V.",
            assigned: "Admin",
            createdBy: "Richard V.",
            createDate: "16 March, 2021",
            status: "Inprogress",
            priority: "High",
        },
        {
            srNo: "13",
            FixHeadId: "VLZ-466",
            purchaseId: "VLZ1400089015",
            title: "Issue with finding information about order ?",
            user: "Olive Gunther",
            assigned: "Alexis Clarke",
            createdBy: "Schaefer",
            createDate: "32 March, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "14",
            FixHeadId: "VLZ-467",
            purchaseId: "VLZ1400090324",
            title: "Make a creating an account profile",
            user: "Edwin",
            assigned: "Admin",
            createdBy: "Edwin",
            createDate: "05 April, 2022",
            status: "Inprogress",
            priority: "Low",
        },
    ];
    return (
        <DataTable
            columns={columns}
            data={data}
            pagination
        />
    );
};

const ModalDataDatatables = () => {
    const columns = [
        {
            name: <Input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />,
            cell: () => (
                <input className="form-check-input fs-15" type="checkbox" name="checkAll" value="option1" />
            ),
        },
        {
            name: <span className='font-weight-bold fs-13'>SR No.</span>,
            selector: row => row.srNo,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>ID</span>,
            selector: row => row.modalId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Purchase ID</span>,
            selector: row => row.purchaseId,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Title</span>,
            selector: row => (<Link to="#!">{row.title}</Link>),
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>User</span>,
            selector: row => row.user,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Assigned To</span>,
            selector: row => row.assigned,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create By</span>,
            selector: row => row.createdBy,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Create Date</span>,
            selector: row => row.createDate,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Status</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.status) {
                    case "Re-open":
                        return <span className="badge badge-soft-info"> {cell.status} </span>;
                    case "On-Hold":
                        return <span className="badge badge-soft-secondary"> {cell.status} </span>;
                    case "Closed":
                        return <span className="badge badge-soft-danger"> {cell.status} </span>;
                    case "Inprogress":
                        return <span className="badge badge-soft-warning"> {cell.status} </span>;
                    case "Open":
                        return <span className="badge badge-soft-primary"> {cell.status} </span>;
                    case "New":
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                    default:
                        return <span className="badge badge-soft-success"> {cell.status} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Priority</span>,
            sortable: true,
            selector: (cell) => {
                switch (cell.priority) {
                    case "High":
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                    case "Medium":
                        return <span className="badge bg-info"> {cell.priority} </span>;
                    case "Low":
                        return <span className="badge bg-success"> {cell.priority} </span>;
                    default:
                        return <span className="badge bg-danger"> {cell.priority} </span>;
                }
            },
        },
        {
            name: <span className='font-weight-bold fs-13'>Action</span>,
            sortable: true,

            cell: () => {
                return (
                    <UncontrolledDropdown className="dropdown d-inline-block">
                        <DropdownToggle className="btn btn-soft-secondary btn-sm" tag="button">
                            <i className="ri-more-fill align-middle"></i>
                        </DropdownToggle>
                        <DropdownMenu className="dropdown-menu-end">
                            <DropdownItem href="#!"><i className="ri-eye-fill align-bottom me-2 text-muted"></i>View</DropdownItem>
                            <DropdownItem className='edit-item-btn'><i className="ri-pencil-fill align-bottom me-2 text-muted"></i>Edit</DropdownItem>
                            <DropdownItem className='remove-item-btn'> <i className="ri-delete-bin-fill align-bottom me-2 text-muted"></i> Delete </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                );
            },
        },
    ];
    const data = [
        {
            srNo: "01",
            modalId: "VLZ-452",
            purchaseId: "VLZ1400087402",
            title: "Post launch reminder/ post list",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "03 Oct, 2021",
            status: "Re-open",
            priority: "High",
        },
        {
            srNo: "02",
            modalId: "VLZ-453",
            purchaseId: "VLZ1400087425",
            title: "Additional Calendar",
            user: "Diana Kohler",
            assigned: "Admin",
            createdBy: "Mary Rucker",
            createDate: "05 Oct, 2021",
            status: "On-Hold",
            priority: "Medium",
        },
        {
            srNo: "03",
            modalId: "VLZ-454",
            purchaseId: "VLZ1400087438",
            title: "Make a creating an account profile",
            user: "Tonya Noble",
            assigned: "Admin",
            createdBy: "Tonya Noble",
            createDate: "27 April, 2022",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "04",
            modalId: "VLZ-455",
            purchaseId: "VLZ1400087748",
            title: "Apologize for shopping Error!",
            user: "Joseph Parker",
            assigned: "Alexis Clarke",
            createdBy: "Joseph Parker",
            createDate: "14 June, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "05",
            modalId: "VLZ-456",
            purchaseId: "VLZ1400087547",
            title: "Support for theme",
            user: "Donald Palmer",
            assigned: "Admin",
            createdBy: "Donald Palmer",
            createDate: "25 June, 2021",
            status: "Closed",
            priority: "Low",
        },
        {
            srNo: "06",
            modalId: "VLZ-457",
            purchaseId: "VLZ1400087245",
            title: "Benner design for FB & Twitter",
            user: "Mary Rucker",
            assigned: "Jennifer Carter",
            createdBy: "Mary Rucker",
            createDate: "14 Aug, 2021",
            status: "Inprogress",
            priority: "Medium",
        },
        {
            srNo: "07",
            modalId: "VLZ-458",
            purchaseId: "VLZ1400087785",
            title: "Change email option process",
            user: "James Morris",
            assigned: "Admin",
            createdBy: "James Morris",
            createDate: "12 March, 2022",
            status: "Open",
            priority: "High",
        },
        {
            srNo: "08",
            modalId: "VLZ-460",
            purchaseId: "VLZ1400087745",
            title: "Support for theme",
            user: "Nathan Cole",
            assigned: "Nancy Martino",
            createdBy: "Nathan Cole",
            createDate: "28 Feb, 2022",
            status: "On-Hold",
            priority: "Low",
        },
        {
            srNo: "09",
            modalId: "VLZ-461",
            purchaseId: "VLZ1400087179",
            title: "Form submit issue",
            user: "Grace Coles",
            assigned: "Admin",
            createdBy: "Grace Coles",
            createDate: "07 Jan, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "10",
            modalId: "VLZ-462",
            purchaseId: "VLZ140008856",
            title: "Edit customer testimonial",
            user: "Freda",
            assigned: "Alexis Clarke",
            createdBy: "Freda",
            createDate: "16 Aug, 2021",
            status: "Closed",
            priority: "Medium",
        },
        {
            srNo: "11",
            modalId: "VLZ-463",
            purchaseId: "VLZ1400078031",
            title: "Ca i have an e-copy invoice",
            user: "Williams",
            assigned: "Admin",
            createdBy: "Williams",
            createDate: "24 Feb, 2022",
            status: "Open",
            priority: "Low",
        },
        {
            srNo: "12",
            modalId: "VLZ-464",
            purchaseId: "VLZ1400087416",
            title: "Brand logo design",
            user: "Richard V.",
            assigned: "Admin",
            createdBy: "Richard V.",
            createDate: "16 March, 2021",
            status: "Inprogress",
            priority: "High",
        },
        {
            srNo: "13",
            modalId: "VLZ-466",
            purchaseId: "VLZ1400089015",
            title: "Issue with finding information about order ?",
            user: "Olive Gunther",
            assigned: "Alexis Clarke",
            createdBy: "Schaefer",
            createDate: "32 March, 2022",
            status: "New",
            priority: "High",
        },
        {
            srNo: "14",
            modalId: "VLZ-467",
            purchaseId: "VLZ1400090324",
            title: "Make a creating an account profile",
            user: "Edwin",
            assigned: "Admin",
            createdBy: "Edwin",
            createDate: "05 April, 2022",
            status: "Inprogress",
            priority: "Low",
        },
    ];
    return (
        <DataTable
            columns={columns}
            data={data}
            pagination
        />
    );
};

const AjaxDatatables = () => {
    const columns = [
        {
            name: <span className='font-weight-bold fs-13'>Name</span>,
            selector: row => row.name,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Position</span>,
            selector: row => row.position,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Office</span>,
            selector: row => row.office,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Extn.</span>,
            selector: row => row.extn,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Start date</span>,
            selector: row => row.startDate,
            sortable: true
        },
        {
            name: <span className='font-weight-bold fs-13'>Salary</span>,
            selector: row => row.salary,
            sortable: true
        },
    ];

    const data = useMemo(() =>[
        {
            id: 1,
            name: "Tiger Nixon",
            position: "System Architect",
            office: "Edinburgh",
            extn: "5421",
            startDate: "2011/04/25",
            salary: "$320,800"
        },
        {
            id: 2,
            name: "Garrett Winters",
            position: "Accountant",
            office: "Tokyo",
            extn: "8422",
            startDate: "2011/07/25",
            salary: "$170,750"
        },
        {
            id: 3,
            name: "Ashton Cox",
            position: "Junior Technical Author",
            office: "San Francisco",
            extn: "1562",
            startDate: "2009/01/12",
            salary: "$86,000"
        },
        {
            id: 4,
            name: "Cedric Kelly",
            position: "Senior Javascript Developer",
            office: "Edinburgh",
            extn: "6224",
            startDate: "2012/03/29",
            salary: "$433,060"
        },
        {
            id: 5,
            name: "Airi Satou",
            position: "Accountant",
            office: "Tokyo",
            extn: "5407",
            startDate: "2008/11/28",
            salary: "$162,700"
        },
        {
            id: 6,
            name: "Brielle Williamson",
            position: "Integration Specialist",
            office: "New York",
            extn: "4804",
            startDate: "2012/12/02",
            salary: "$372,000"
        },
        {
            id: 7,
            name: "Herrod Chandler",
            position: "Sales Assistant",
            office: "San Francisco",
            extn: "9608",
            startDate: "2012/08/06",
            salary: "$137,500"
        },
        {
            id: 8,
            name: "Rhona Davidson",
            position: "Integration Specialist",
            office: "Tokyo",
            extn: "6200",
            startDate: "2010/10/14",
            salary: "$327,900"
        },
        {
            id: 9,
            name: "Colleen Hurst",
            position: "Javascript Developer",
            office: "San Francisco",
            extn: "2360",
            startDate: "2009/09/15",
            salary: "$205,500"
        },
        {
            id: 10,
            name: "Sonya Frost",
            position: "Software Engineer",
            office: "Edinburgh",
            extn: "1667",
            startDate: "2008/12/13",
            salary: "$103,600",
        },
        {
            id: 11,
            name: "Jena Gaines",
            position: "Office Manager",
            office: "London",
            extn: "3814",
            startDate: "2008/12/19",
            salary: "$90,560",
        },
        {
            id: 12,
            name: "Quinn Flynn",
            position: "Support Lead",
            office: "Edinburgh",
            extn: "9497",
            startDate: "2013/03/03",
            salary: "$342,000",
        },
        {
            id: 13,
            name: "Charde Marshall",
            position: "Regional Director",
            office: "San Francisco",
            extn: "6741",
            startDate: "2008/10/16",
            salary: "$470,600"
        },
        {
            id: 14,
            name: "Haley Kennedy",
            position: "Senior Marketing Designer",
            office: "London",
            extn: "3597",
            startDate: "2012/12/18",
            salary: "$313,500"
        },
        {
            id: 15,
            name: "Tatyana Fitzpatrick",
            position: "Regional Director",
            office: "London",
            extn: "1965",
            startDate: "2010/03/17",
            salary: "$385,750",
        },
    ], []);

    const [pending, setPending] = useState(true);
    const [rows, setRows] = useState([]);
    useEffect(() => {
        const timeout = setTimeout(() => {
            setRows(data);
            setPending(false);
        }, 2000);
        return () => clearTimeout(timeout);
    }, [data]);

    return (
        <DataTable
            columns={columns}
            pagination
            data={rows}
            progressPending={pending}
        />
    );
};


export { BasicTable, ScrollVertical, ScrollHorizontal, AlternativePagination, FixedHeaderDatatables, ModalDataDatatables, AjaxDatatables };