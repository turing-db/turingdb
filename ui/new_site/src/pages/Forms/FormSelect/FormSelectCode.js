import React from 'react';
import PrismCode from '../../../Components/Common/Prism';

//DefaultSelect code
const DefaultSelect = () => {

    const code =
        `<Row>
<Col lg={6}>
    <select className="form-select mb-3" aria-label="Default select example">
        <option >Select your Status </option>
        <option defaultValue="1">Declined Payment</option>
        <option defaultValue="2">Delivery Error</option>
        <option defaultValue="3">Wrong Amount</option>
    </select>
</Col>
<Col lg={6}>
    <select className="form-select rounded-pill mb-3" aria-label="Default select example">
        <option >Search for services</option>
        <option defaultValue="1">Information Architecture</option>
        <option defaultValue="2">UI/UX Design</option>
        <option defaultValue="3">Back End Development</option>
    </select>
</Col>
<Col lg={6}>
    <select className="form-select" aria-label="Disabled select example" disabled>
        <option >Open this select menu (Disabled)</option>
        <option defaultValue="1">One</option>
        <option defaultValue="2">Two</option>
        <option defaultValue="3">Three</option>
    </select>
</Col>
</Row>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//MenuSize code
const MenuSize = () => {

    const code =
        `<Row>
    <Col lg={6}>
        <select className="form-select mb-3" aria-label="Default select example">
            <option >Select your Status </option>
            <option defaultValue="1">Declined Payment</option>
            <option defaultValue="2">Delivery Error</option>
            <option defaultValue="3">Wrong Amount</option>
        </select>
    </Col>
    <Col lg={6}>
        <select className="form-select rounded-pill mb-3" aria-label="Default select example">
            <option >Search for services</option>
            <option defaultValue="1">Information Architecture</option>
            <option defaultValue="2">UI/UX Design</option>
            <option defaultValue="3">Back End Development</option>
        </select>
    </Col>
    <Col lg={6}>
        <select className="form-select" aria-label="Disabled select example" disabled>
            <option >Open this select menu (Disabled)</option>
            <option defaultValue="1">One</option>
            <option defaultValue="2">Two</option>
            <option defaultValue="3">Three</option>
        </select>
    </Col>
    </Row>
    `;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

//SelectSize code
const SelectSize = () => {

    const code =
        `<Row className="align-items-center g-3">
<Col lg={4}>
    <select className="form-select form-select-sm" aria-label=".form-select-sm example">
        <option selected>Open this select menu</option>
        <option defaultValue="1">One</option>
        <option defaultValue="2">Two</option>
        <option defaultValue="3">Three</option>
    </select>
</Col>
<Col lg={4}>
    <select className="form-select" aria-label=".form-select-sm example">
        <option selected>Open this select menu</option>
        <option defaultValue="1">One</option>
        <option defaultValue="2">Two</option>
        <option defaultValue="3">Three</option>
    </select>
</Col>
<Col lg={4}>
    <select className="form-select form-select-lg" aria-label=".form-select-lg example">
        <option selected>Open this select menu</option>
        <option defaultValue="1">One</option>
        <option defaultValue="2">Two</option>
        <option defaultValue="3">Three</option>
    </select>
</Col>
</Row>
`;
    return (
        <React.Fragment>
            <PrismCode
                code={code}
                 language={"react"}
            />
        </React.Fragment>
    );
};

export { DefaultSelect, MenuSize, SelectSize };