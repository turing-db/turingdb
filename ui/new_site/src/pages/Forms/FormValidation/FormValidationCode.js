import React from 'react';
import PrismCode from '../../../Components/Common/Prism';

//BrowserDefaults code
const BrowserDefaults = () => {
  const code = `<form className="row g-3">
<Col md={4}>
    <Label htmlFor="validationDefault01" className="form-label">First name</Label>
    <Input type="text" className="form-control" id="validationDefault01" defaultValue="Mark" required/>
</Col>
<Col md={4}>
    <Label htmlFor="validationDefault02" className="form-label">Last name</Label>
    <Input type="text" className="form-control" id="validationDefault02" defaultValue="Otto" required/>
</Col>
<Col md={4}>
    <label htmlFor="validationDefaultUsername" className="form-label">Username</label>
    <InputGroup>
        <span className="input-group-text" id="inputGroupPrepend2">@</span>
        <Input type="text" className="form-control" id="validationDefaultUsername" aria-describedby="inputGroupPrepend2"
            required />
    </InputGroup>
</Col>
<Col md={6}>
    <Label htmlFor="validationDefault03" className="form-label">City</Label>
    <Input type="text" className="form-control" id="validationDefault03" required/>
</Col>
<Col md={3}>
    <Label htmlFor="validationDefault04" className="form-label">State</Label>
    <select className="form-select" id="validationDefault04" required>
        <option disabled defaultValue="">Choose...</option>
        <option>...</option>
    </select>
</Col>
<Col md={3}>
    <Label htmlFor="validationDefault05" className="form-label">Zip</Label>
    <Input type="text" className="form-control" id="validationDefault05" required/>
</Col>
<Col xs={12}>
    <div className="form-check">
        <Input className="form-check-input" type="checkbox" defaultValue="" id="invalidCheck2" required/>
        <Label className="form-check-label" htmlFor="invalidCheck2">
            Agree to terms and conditions
        </Label>
    </div>
</Col>
<Col xs={12}>
    <button className="btn btn-primary" type="submit">Submit form</button>
</Col>
</form>
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

//CustomStyles code
const CustomStyles = () => {
  const code = `<Form className="needs-validation"
onSubmit={(e) => {
  e.preventDefault();
  validation.handleSubmit();
  return false;
}}
>
<Row>
  <Col md="6">
    <FormGroup className="mb-3">
      <Label htmlFor="validationCustom01">First name</Label>
      <Input
        name="firstname"
        placeholder="Mark"
        type="text"
        className="form-control"
        id="validationCustom01"
        onChange={validation.handleChange}
        onBlur={validation.handleBlur}
        value={validation.values.firstname || ""}
        invalid={
          validation.touched.firstname &&
            validation.errors.firstname
            ? true
            : false
        }
      />
      {validation.touched.firstname &&
        validation.errors.firstname ? (
        <FormFeedback type="invalid">
          {validation.errors.firstname}
        </FormFeedback>
      ) : null}
    </FormGroup>
  </Col>
  <Col md="6">
    <FormGroup className="mb-3">
      <Label htmlFor="validationCustom02">Last name</Label>
      <Input
        name="lastname"
        placeholder="Otto"
        type="text"
        className="form-control"
        id="validationCustom02"
        onChange={validation.handleChange}
        onBlur={validation.handleBlur}
        value={validation.values.lastname || ""}
        invalid={
          validation.touched.lastname &&
            validation.errors.lastname
            ? true
            : false
        }
      />
      {validation.touched.lastname &&
        validation.errors.lastname ? (
        <FormFeedback type="invalid">
          {validation.errors.lastname}
        </FormFeedback>
      ) : null}
    </FormGroup>
  </Col>
</Row>
<Row>
  <Col md="4">
    <FormGroup className="mb-3">
      <Label htmlFor="validationCustom03">City</Label>
      <Input
        name="city"
        placeholder="City"
        type="text"
        className="form-control"
        onChange={validation.handleChange}
        onBlur={validation.handleBlur}
        value={validation.values.city || ""}
        invalid={
          validation.touched.city && validation.errors.city
            ? true
            : false
        }
      />
      {validation.touched.city && validation.errors.city ? (
        <FormFeedback type="invalid">
          {validation.errors.city}
        </FormFeedback>
      ) : null}
    </FormGroup>
  </Col>
  <Col md="4">
    <FormGroup className="mb-3">
      <Label htmlFor="validationCustom04">State</Label>
      <Input
        name="state"
        placeholder="State"
        type="text"
        className="form-control"
        id="validationCustom04"
        onChange={validation.handleChange}
        onBlur={validation.handleBlur}
        value={validation.values.state || ""}
        invalid={
          validation.touched.state &&
            validation.errors.state
            ? true
            : false
        }
      />
      {validation.touched.state &&
        validation.errors.state ? (
        <FormFeedback type="invalid">
          {validation.errors.state}
        </FormFeedback>
      ) : null}
    </FormGroup>
  </Col>
  <Col md="4">
    <FormGroup className="mb-3">
      <Label htmlFor="validationCustom05">Zip</Label>
      <Input
        name="zip"
        placeholder="Zip"
        type="text"
        className="form-control"
        id="validationCustom05"
        onChange={validation.handleChange}
        onBlur={validation.handleBlur}
        value={validation.values.zip || ""}
        invalid={
          validation.touched.zip && validation.errors.zip
            ? true
            : false
        }
      />
      {validation.touched.zip && validation.errors.zip ? (
        <FormFeedback type="invalid">
          {validation.errors.zip}
        </FormFeedback>
      ) : null}
    </FormGroup>
  </Col>
</Row>
<Row>
  <Col lg="12">
    <FormGroup className="mb-3">
      <div className="form-check">
        <Input
          type="checkbox"
          className="form-check-input"
          id="invalidCheck"
        />
        <Label
          className="form-check-label"
          htmlFor="invalidCheck"
        >
          {" "}
          Agree to terms and conditions
        </Label>
      </div>
    </FormGroup>
  </Col>
</Row>
<Button color="primary" type="submit">
  Submit form
</Button>
</Form>
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
//Supported Elements code
const SupportedElements = () => {
  const code = `<Form className="was-validated">
<div className="mb-3">
    <Label htmlFor="validationTextarea" className="form-label">Textarea</Label>
       <textarea className="form-control" id="validationTextarea" placeholder="Required example textarea"
    required></textarea>
    <div className="invalid-feedback">
    Please enter a message in the textarea.
    </div>
</div>

<div className="form-check mb-3">
    <Input type="checkbox" className="form-check-input" id="validationFormCheck1" required />
    <Label className="form-check-label" htmlFor="validationFormCheck1">Check this checkbox</Label>
    <div className="invalid-feedback">Example invalid feedback text</div>
</div>

<div className="form-check">
    <Input type="radio" className="form-check-input" id="validationFormCheck2" name="radio-stacked" required />
    <Label className="form-check-label" htmlFor="validationFormCheck2">Toggle this radio</Label>
</div>
<div className="form-check mb-3">
    <Input type="radio" className="form-check-input" id="validationFormCheck3" name="radio-stacked" required />
    <Label className="form-check-label" for="validationFormCheck3">Or toggle this other radio</Label>
    <div className="invalid-feedback">More example invalid feedback text</div>
</div>

<div className="mb-3">
    <select className="form-select" required aria-label="select example">
    <option value="">Open this select menu</option>
    <option value="1">One</option>
    <option value="2">Two</option>
    <option value="3">Three</option>
    </select>
    <div className="invalid-feedback">Example invalid select feedback</div>
</div>

<div className="mb-3">
    <Input type="file" className="form-control" aria-label="file example" required />
    <div className="invalid-feedback">Example invalid form file feedback</div>
</div>

<div className="mb-0">
    <button className="btn btn-primary" type="submit" disabled>Submit form</button>
</div>
</Form>
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

export { BrowserDefaults, CustomStyles, SupportedElements };