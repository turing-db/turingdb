import React from 'react';
import { Grid, _ } from 'gridjs-react';

const data = [
    ["01", "Jonathan", "jonathan@example.com", "Senior Implementation Architect", "Hauck Inc", "Holy See"],
    ["02", "Harold", "harold@example.com", "Forward Creative Coordinator", "Metz Inc", "Iran"],
    ["03", "Shannon", "shannon@example.com", "Legacy Functionality Associate", "Zemlak Group", "South Georgia"],
    ["04", "Robert", "robert@example.com", "Product Accounts Technician", "Hoeger", "San Marino"],
    ["05", "Noel", "noel@example.com", "Customer Data Director", "Howell - Rippin", "Germany"],
    ["06", "Traci", "traci@example.com", "Corporate Identity Director", "Koelpin - Goldner", "Vanuatu"],
    ["07", "Kerry", "kerry@example.com", "Lead Applications Associate", "Feeney, Langworth and Tremblay", "Niger"],
    ["08", "Patsy", "patsy@example.com", "Dynamic Assurance Director", "Streich Group", "Niue"],
    ["09", "Cathy", "cathy@example.com", "Customer Data Director", "Ebert, Schamberger and Johnston", "Mexico"],
    ["10", "Tyrone", "tyrone@example.com", "Senior Response Liaison", "Raynor, Rolfson and Daugherty", "Qatar"],
];

const data1 = [
    ["#VL2111", "Jonathan", "07 Oct, 2021", "$24.05", "Paid",],
    ["#VL2110", "Harold", "07 Oct, 2021", "$26.15", "Paid"],
    ["#VL2109", "Shannon", "06 Oct, 2021", "$21.25", "Refund"],
    ["#VL2108", "Robert", "05 Oct, 2021", "$25.03", "Paid"],
    ["#VL2107", "Noel", "05 Oct, 2021", "$22.61", "Paid"],
    ["#VL2106", "Traci", "04 Oct, 2021", "$24.05", "Paid"],
    ["#VL2105", "Kerry", "04 Oct, 2021", "$26.15", "Paid"],
    ["#VL2104", "Patsy", "04 Oct, 2021", "$21.25", "Refund"],
    ["#VL2103", "Cathy", "03 Oct, 2021", "$22.61", "Paid"],
    ["#VL2102", "Tyrone", "03 Oct, 2021", "$25.03", "Paid"],
];

const data2 = [
    ["Jonathan", "jonathan@example.com", "Senior Implementation Architect", "Hauck Inc", "Holy See"],
    ["Harold", "harold@example.com", "Forward Creative Coordinator", "Metz Inc", "Iran"],
    ["Shannon", "shannon@example.com", "Legacy Functionality Associate", "Zemlak Group", "South Georgia"],
    ["Robert", "robert@example.com", "Product Accounts Technician", "Hoeger", "San Marino"],
    ["Noel", "noel@example.com", "Customer Data Director", "Howell - Rippin", "Germany"],
    ["Traci", "traci@example.com", "Corporate Identity Director", "Koelpin - Goldner", "Vanuatu"],
    ["Kerry", "kerry@example.com", "Lead Applications Associate", "Feeney, Langworth and Tremblay", "Niger"],
    ["Patsy", "patsy@example.com", "Dynamic Assurance Director", "Streich Group", "Niue"],
    ["Cathy", "cathy@example.com", "Customer Data Director", "Ebert, Schamberger and Johnston", "Mexico"],
    ["Tyrone", "tyrone@example.com", "Senior Response Liaison", "Raynor, Rolfson and Daugherty", "Qatar"],
];

// Base Example
const BaseExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={data}
                columns={[{
                    name: 'ID',
                    formatter: (cell) => _(<span className="fw-semibold">{cell}</span>)
                },
                    "Name",
                {
                    name: 'Email',
                    formatter: (cell) => _(<a href="/#"> {cell} </a>)
                },
                    "Position", "Company", "Country",
                {
                    name: 'Actions',
                    width: '120px',
                    formatter: (cell) => _(<a href='/#' className='text-reset text-decoration-underline'> Details </a>)
                },
                ]}
                search={true}
                sort={true}
                pagination={{ enabled: true, limit: 5, }}
            />
        </React.Fragment>
    );
};

// Card Table
const CardTableExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={data2}
                columns={["Name", "Email", "Position", "Company", "Country"]}
                sort={true}
                pagination={{ enabled: true, limit: 5, }}
            />
        </React.Fragment>
    );
};

// Pagination
const PaginationExample = () => {
    return (
        <React.Fragment>
            <React.Fragment>
                <Grid
                    data={data1}
                    columns={[{
                        name: 'ID',
                        width: '120px',
                        formatter: (cell) => _(<a href="/#" className="fw-medium"> {cell} </a>)
                    }, "Name", "Date", "Total", "Status",
                    {
                        name: 'Actions',
                        width: '100px',
                        formatter: (cell) => _(<button type='button' className='btn btn-sm btn-light'> Details </button>)
                    },
                    ]}
                    pagination={{ enabled: true, limit: 5, }}
                />
            </React.Fragment>
        </React.Fragment>
    );
};

// Search
const SearchExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={data2}
                columns={["Name", "Email", "Position", "Company", "Country"]}
                search={true}
                pagination={{ enabled: true, limit: 5, }}
            />
        </React.Fragment>
    );
};

// Sorting
const SortingExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={data2}
                columns={["Name", "Email", "Position", "Company", "Country"]}
                sort={true}
                pagination={{ enabled: true, limit: 5, }}
            />
        </React.Fragment>
    );
};

// Loading State
const LoadingStateExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={function () {
                    return new Promise(function (resolve) {
                        setTimeout(function () {
                            resolve(data2);
                        }, 2000);
                    });
                }}
                columns={[
                    "Name", "Email", "Position", "Company", "Country"]}
                sort={true}
                pagination={{ enabled: true, limit: 5, }}
            />            
        </React.Fragment>
    );
};

// Fixed Header
const FixedHeaderExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={data2}
                columns={[
                    "Name", "Email", "Position", "Company", "Country"]}
                sort={true}
                pagination={{ enabled: true, limit: 10, }}
                fixedHeader={true}
                height={'400px'}
            />
        </React.Fragment>
    );
};

// Hidden Columns
const HiddenColumnsExample = () => {
    return (
        <React.Fragment>
            <Grid
                data={data}
                columns={[
                    {
                        name: 'ID',
                        hidden: true
                    },
                    "Name", "Email", "Position", "Company", {
                        name: 'Country',
                        hidden: true
                    }
                ]}
                sort={true}
                pagination={{ enabled: true, limit: 5, }}
            />
        </React.Fragment>
    );
};



export { BaseExample, CardTableExample, PaginationExample, SearchExample, SortingExample, LoadingStateExample, FixedHeaderExample, HiddenColumnsExample };
