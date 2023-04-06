import React from 'react';
import UiContent from "../../../Components/Common/UiContent";
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import { Card, CardBody, Col, Container, Input, Label, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import {
    Checkbox, CustomCheckbox, Radio, CustomRadio, Switches, SwitchColor, CustomSwitches, InlineCheckboxRadio, WithoutLabels,
    RadioToggleButtons, OutlinedStyles
} from '../CheckboxAndRadio/CheckboxAndRadioCode';


const CheckBoxAndRadio = () => {
    document.title="Checkbox & Radio | Velzon - React Admin & Dashboard Template";
    
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">               
                <Container fluid>
                    <BreadCrumb title="Checkbox & Radio" pageTitle="Forms" />
                    <Row>
                        <Col lg={12} >
                            <Card>
                                <PreviewCardHeader title="Checkbox" />
                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={4} md={6}>
                                                <div>
                                                    <p className="text-muted fw-medium">Default Checkbox</p>
                                                    <p className="text-muted">Use <code>type="checkbox"</code> attribute to set a checkbox and add <code>checked</code> attribute to set a checkbox checked by default.</p>
                                                    <div className="form-check mb-2">
                                                        <Input className="form-check-input" type="checkbox" id="formCheck1" />
                                                        <Label className="form-check-label" htmlFor="formCheck1">
                                                            Default checkbox
                                                        </Label>
                                                    </div>
                                                    <div className="form-check">
                                                        <Input className="form-check-input" type="checkbox" id="formCheck2" defaultChecked />                                                           /
                                                        <Label className="form-check-label" htmlFor="formCheck2">
                                                            Checked checkbox
                                                        </Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4} md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted fw-medium">Disabled Checkbox</p>
                                                    <p className="text-muted">Use <code>disabled</code> attribute to set a checkbox disabled and add <code>checked</code> attribute to set a checkbox checked by default.</p>
                                                    <div>
                                                        <div className="form-check form-check-right mb-2">
                                                            <Input className="form-check-input" type="checkbox" value="" id="flexCheckDisabled" disabled />
                                                            <Label className="form-check-label" htmlFor="flexCheckDisabled">
                                                                Disabled checkbox
                                                            </Label>
                                                        </div>
                                                    </div>
                                                    <div>
                                                        <div className="form-check form-check-right">
                                                            <Input className="form-check-input" type="checkbox" value="" id="flexCheckCheckedDisabled" defaultChecked disabled />
                                                            <Label className="form-check-label" htmlFor="flexCheckCheckedDisabled">
                                                                Disabled checked checkbox
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4} md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted fw-medium">Checkbox Right</p>
                                                    <p className="text-muted">Use <code>form-check-right</code> class to form-check class to set a checkbox on the right side.</p>
                                                    <div className="form-check form-check-right mb-2">
                                                        <Input className="form-check-input" type="checkbox" name="formCheckboxRight" id="formCheckboxRight1" />
                                                        <Label className="form-check-label" htmlFor="formCheckboxRight1">
                                                            Form Radio Right
                                                        </Label>
                                                    </div>
                                                    <div>
                                                        <div className="form-check form-check-right">
                                                            <Input className="form-check-input" type="checkbox" value="" id="flexCheckCheckedRightDisabled" defaultChecked disabled />
                                                            <Label className="form-check-label" htmlFor="flexCheckCheckedRightDisabled">
                                                                Disabled checked checkbox
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={6}>
                                                <div className="mt-3">
                                                    <p className="text-muted fw-medium">Indeterminate</p>
                                                    <div>
                                                        <div className="form-check">
                                                            <Input className="form-check-input" type="checkbox" value="" id="defaultIndeterminateCheck" />
                                                            <Label className="form-check-label" htmlFor="defaultIndeterminateCheck">
                                                                Indeterminate checkbox
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <Checkbox />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <div className="col-12">
                            <Card>
                                <PreviewCardHeader title="Custom Checkbox" />
                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col md={6}>
                                                <div>
                                                    <p className="text-muted">Use <code>form-check-</code> class with below-mentioned color variation to set a color checkbox.</p>
                                                    <div>
                                                        <div className="form-check mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck6" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck6">
                                                                Checkbox Primary
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-secondary mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck7" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck7">
                                                                Checkbox Secondary
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-success mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck8" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck8">
                                                                Checkbox Success
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-warning mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck9" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck9">
                                                                Checkbox Warning
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-danger mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck10" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck10">
                                                                Checkbox Danger
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-info mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck11" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck11">
                                                                Checkbox Info
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-dark">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck12" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck12">
                                                                Checkbox Dark
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted">Use <code>form-check-outline</code> class and <code>form-check-</code> class with below-mentioned color variation to set a color checkbox with outline.</p>

                                                    <div>
                                                        <div className="form-check form-check-outline form-check-primary mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck13" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck13">
                                                                Checkbox Outline Primary
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-outline form-check-secondary mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck14" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck14">
                                                                Checkbox Outline Secondary
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-outline form-check-success mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck15" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck15">
                                                                Checkbox Outline Success
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-outline form-check-warning mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck16" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck16">
                                                                Checkbox Outline Warning
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-outline form-check-danger mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck17" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck17">
                                                                Checkbox Outline Danger
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-outline form-check-info mb-3">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck18" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck18">
                                                                Checkbox Outline Info
                                                            </Label>
                                                        </div>
                                                        <div className="form-check form-check-outline form-check-dark">
                                                            <Input className="form-check-input" type="checkbox" id="formCheck19" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="formCheck19">
                                                                Checkbox Outline Dark
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <CustomCheckbox />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </div>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Radio" />
                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={4} md={6}>
                                                <div>
                                                    <p className="text-muted fw-medium">Default Radios</p>
                                                    <p className="text-muted">Use <code>type="radio"</code> attribute to set a radio button and add <code>checked</code> attribute to set a radio checked by default.</p>
                                                    <div className="form-check mb-2">
                                                        <Input className="form-check-input" type="radio" name="flexRadioDefault" id="flexRadioDefault1" />
                                                        <Label className="form-check-label" htmlFor="flexRadioDefault1">
                                                            Default radio
                                                        </Label>
                                                    </div>
                                                    <div className="form-check">
                                                        <Input className="form-check-input" type="radio" name="flexRadioDefault" id="flexRadioDefault2" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="flexRadioDefault2">
                                                            Default checked radio
                                                        </Label>
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col lg={4} md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted fw-medium">Disabled Radios</p>
                                                    <p className="text-muted">Use <code>disabled</code> attribute to set a radio button disabled and add <code>defaultChecked</code> attribute to set a radio checked by default.</p>
                                                    <div className="form-check mb-2">
                                                        <Input className="form-check-input" type="radio" name="flexRadioDisabled" id="flexRadioDisabled" disabled />
                                                        <Label className="form-check-label" htmlFor="flexRadioDisabled">
                                                            Disabled radio
                                                        </Label>
                                                    </div>
                                                    <div>
                                                        <div className="form-check">
                                                            <Input className="form-check-input" type="radio" name="flexRadioDisabled" id="flexRadioCheckedDisabled" defaultChecked disabled />
                                                            <Label className="form-check-label" htmlFor="flexRadioCheckedDisabled">
                                                                Disabled checked radio
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4} md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted fw-medium">Radio Right</p>
                                                    <p className="text-muted">Use <code>form-check-right</code> class to form-check class to set a radio button on the right side.</p>
                                                    <div className="form-check form-check-right mb-2">
                                                        <Input className="form-check-input" type="radio" name="formradioRight" id="formradioRight1" />
                                                        <Label className="form-check-label" htmlFor="formradioRight1">
                                                            Form Radio Right
                                                        </Label>
                                                    </div>
                                                    <div>
                                                        <div className="form-check form-check-right">
                                                            <Input className="form-check-input" type="radio" value="" name="formradioRight" id="flexCheckCheckedDisabled2" defaultChecked disabled />
                                                            <Label className="form-check-label" htmlFor="flexCheckCheckedDisabled2">
                                                                Disabled checked radio
                                                            </Label>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <Radio />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <div className="col-12">
                            <Card>
                                <PreviewCardHeader title="Custom Radio" />
                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col md={6}>
                                                <div>
                                                    <p className="text-muted">Use <code>form-check-</code> class with below-mentioned color variation to set a color radio.</p>

                                                    <div className="form-check form-radio-primary mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor1" id="formradioRight5" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight5">
                                                            Radio Primary
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-secondary mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor2" id="formradioRight6" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight6">
                                                            Radio Secondary
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-success mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor3" id="formradioRight7" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight7">
                                                            Radio Success
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-warning mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor4" id="formradioRight8" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight8">
                                                            Radio Warning
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-danger mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor5" id="formradioRight9" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight9">
                                                            Radio Danger
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-info mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor6" id="formradioRight10" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight10">
                                                            Radio Info
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-dark">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor7" id="formradioRight11" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight11">
                                                            Radio Dark
                                                        </Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted">Use <code>form-check-outline</code> class and <code>form-check-</code> class with below-mentioned color variation to set a color radio with outline.</p>

                                                    <div className="form-check form-radio-outline form-radio-primary mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor9" id="formradioRight13" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight13">
                                                            Radio Outline Primary
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-outline form-radio-secondary mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor10" id="formradioRight14" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight14">
                                                            Radio Outline Secondary
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-outline form-radio-success mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor11" id="formradioRight15" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight15">
                                                            Radio Outline Success
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-outline form-radio-warning mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor12" id="formradioRight16" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight16">
                                                            Radio Outline Warning
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-outline form-radio-danger mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor13" id="formradioRight17" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight17">
                                                            Radio Outline Danger
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-outline form-radio-info mb-3">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor14" id="formradioRight18" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight18">
                                                            Radio Outline Info
                                                        </Label>
                                                    </div>

                                                    <div className="form-check form-radio-outline form-radio-dark">
                                                        <Input className="form-check-input" type="radio" name="formradiocolor15" id="formradioRight19" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="formradioRight19">
                                                            Radio Outline Dark
                                                        </Label>
                                                    </div>

                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                            </code>
                                            <CustomRadio />
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </div>
                    </Row>

                    <Row>
                        <div className="col-12">
                            <Card>
                                <PreviewCardHeader title="Switches" />
                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={4} md={6}>
                                                <div>
                                                    <p className="text-muted fw-medium">Default Switches</p>
                                                    <p className="text-muted">Use <code>form-switch</code> class to form-check class to set a switch and add <code>checked</code> attribute to set a switch checked by default.</p>
                                                    <div className="form-check form-switch mb-2">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckDefault" />
                                                        <Label className="form-check-label" htmlFor="flexSwitchCheckDefault">Default switch input</Label>
                                                    </div>

                                                    <div className="form-check form-switch">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckChecked" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="flexSwitchCheckChecked">Checked switch input</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4} md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted fw-medium">Disabled Switches</p>
                                                    <p className="text-muted">Use <code>disabled</code> attribute to set a radio button disabled and add <code>checked</code> attribute to set a switch checked by default.</p>
                                                    <div className="form-check form-switch mb-2">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckDisabled" disabled />
                                                        <Label className="form-check-label" htmlFor="flexSwitchCheckDisabled">Switch input</Label>
                                                    </div>

                                                    <div className="form-check form-switch">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckCheckedDisabled1" defaultChecked disabled />
                                                        <Label className="form-check-label" htmlFor="flexSwitchCheckCheckedDisabled1">Disabled checked switch input</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4} md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <p className="text-muted fw-medium">Switch Right</p>
                                                    <p className="text-muted">Use <code>form-check-right</code> class to form-check class to set a switch button on the right side.</p>
                                                    <div>
                                                        <div className="form-check form-switch form-check-right mb-2">
                                                            <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckRightDisabled" defaultChecked />
                                                            <Label className="form-check-label" htmlFor="flexSwitchCheckRightDisabled">Switch Right input</Label>
                                                        </div>
                                                    </div>

                                                    <div>
                                                        <div className="form-check form-switch form-check-right">
                                                            <Input className="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckCheckedDisabled2" disabled />
                                                            <Label className="form-check-label" htmlFor="flexSwitchCheckCheckedDisabled2">Disabled checked switch input</Label>
                                                        </div>
                                                    </div>

                                                </div>
                                            </Col>

                                            <Col lg={4} md={6}>
                                                <div className="mt-3">
                                                    <p className="text-muted fw-medium">Switch sizes</p>
                                                    <p className="text-muted">Use <code>form-switch-md</code> class to set a medium size switch button and
                                                        <code>form-switch-lg</code> class to form-check class to set a large size switch button respectively.
                                                        No such class is required for small size switch button.</p>

                                                    <div className="form-check form-switch mb-3" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="customSwitchsizesm" defaultChecked="" />
                                                        <Label className="form-check-label" htmlFor="customSwitchsizesm">Small Size Switch</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-md mb-3" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="customSwitchsizemd" />
                                                        <Label className="form-check-label" htmlFor="customSwitchsizemd">Medium Size Switch</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-lg" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="customSwitchsizelg" defaultChecked="" />
                                                        <Label className="form-check-label" htmlFor="customSwitchsizelg">Large Size Switch</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <Switches />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </div>
                    </Row>
                    <Row>
                        <div className="col-12">
                            <Card >
                                <PreviewCardHeader title="Switch Color" />

                                <div className="card-body">
                                    <p className="text-muted">Use <code>form-check-</code> class with below-mentioned color variation to set a color switch.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col md={6}>
                                                <div>
                                                    <div className="form-check form-switch mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck1" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck1">Switch Default</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-secondary mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck2" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck2">Switch Secondary</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-success mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck3" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck3">Switch Success</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-warning mb-2 mb-md-0">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck4" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck4">Switch Warning</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col md={6}>
                                                <div>
                                                    <div className="form-check form-switch form-switch-danger mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck5" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck5">Switch Danger</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-info mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck6" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck6">Switch Info</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-dark mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck7" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck7">Switch Dark</Label>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <SwitchColor />
                                            </code>
                                        </pre>
                                    </div>
                                </div>
                            </Card>
                        </div>
                    </Row>
                    <Row>
                        <div className="col-12">
                            <Card>
                                <PreviewCardHeader title="Custom Switches" />

                                <CardBody >
                                    <p className="text-muted">Use <code>form-switch-custom</code> class &amp; <code>form-switch-</code> class with below-mentioned color
                                        variation to set a color radius.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col md={6}>
                                                <div>
                                                    <div className="form-check form-switch form-switch-custom form-switch-primary mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck9" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck9">Switch Primary</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-custom form-switch-secondary mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck10" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck10">Switch Secondary</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-custom form-switch-success mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck11" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck11">Switch Success</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-custom form-switch-warning mb-2 mb-md-0">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck12" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck12">Switch Warning</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col md={6}>
                                                <div className="mt-4 mt-md-0">
                                                    <div className="form-check form-switch form-switch-custom form-switch-danger mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck13" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck13">Switch Danger</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-custom form-switch-info mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck14" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck14">Switch Info</Label>
                                                    </div>

                                                    <div className="form-check form-switch form-switch-custom form-switch-dark mb-3">
                                                        <Input className="form-check-input" type="checkbox" role="switch" id="SwitchCheck15" defaultChecked />
                                                        <Label className="form-check-label" htmlFor="SwitchCheck15">Switch Dark</Label>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <CustomSwitches />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </div>
                    </Row>
                    <Row >
                        <Col xs={12}>
                            <Card>
                                <PreviewCardHeader title="Inline Checkbox & Radios" />

                                <CardBody>
                                    <p className="text-muted">Use <code>form-check-inline</code> class to form-check class to set horizontally align checkboxes, radios, or switches.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={4}>
                                                <div className="mt-4 mt-lg-0">
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox6" value="option1" />
                                                        <Label className="form-check-label" htmlFor="inlineCheckbox6">1</Label>
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox7" value="option2" />
                                                        <Label className="form-check-label" htmlFor="inlineCheckbox7">2</Label>
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox8" value="option3" disabled />
                                                        <Label className="form-check-label" htmlFor="inlineCheckbox8">3 (disabled)</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4}>
                                                <div className="mt-4 mt-lg-0">
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="radio" name="inlineRadioOptions1" id="inlineRadio1" value="option1" />
                                                        <Label className="form-check-label" htmlFor="inlineRadio1">1</Label>
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="radio" name="inlineRadioOptions2" id="inlineRadio2" value="option2" />
                                                        <Label className="form-check-label" htmlFor="inlineRadio2">2</Label>
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="radio" name="inlineRadioOptions3" id="inlineRadio3" value="option3" disabled />
                                                        <Label className="form-check-label" htmlFor="inlineRadio3">3 (disabled)</Label>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={4}>
                                                <div className="mt-4 mt-lg-0">
                                                    <div className="form-check form-switch form-check-inline" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="inlineswitch5" />
                                                        <Label className="form-check-label" htmlFor="inlineswitch5">1</Label>
                                                    </div>
                                                    <div className="form-check form-switch form-check-inline" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="inlineswitch6" />
                                                        <Label className="form-check-label" htmlFor="inlineswitch6">2</Label>
                                                    </div>
                                                    <div className="form-check form-switch form-check-inline" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="inlineswitchdisabled2" disabled />
                                                        <Label className="form-check-label" htmlFor="inlineswitchdisabled2">2</Label>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <InlineCheckboxRadio />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xs={12}>
                            <Card >
                                <PreviewCardHeader title="Without Labels" />
                                <CardBody >
                                    <p className="text-muted">Omit the wrapping, <code>form-check</code> class for checkboxes, radios, or switches that have no label text. Remember to still provide some form of accessible name for assistive technologies (for instance, using aria-label).</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={4}>
                                                <div className="mt-4 mt-lg-0">
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox1" value="option1" />
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox2" value="option2" />
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="checkbox" id="inlineCheckbox3" value="option3" disabled />
                                                    </div>
                                                </div>
                                            </Col>


                                            <Col lg={4}>
                                                <div className="mt-4 mt-lg-0">
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="WithoutinlineRadio1" value="option1" />
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="WithoutinlineRadio2" value="option2" />
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <Input className="form-check-input" type="radio" name="inlineRadioOptions" id="WithoutinlineRadio3" value="option3" disabled />
                                                    </div>
                                                </div>
                                            </Col>


                                            <Col lg={4}>
                                                <div className="mt-4 mt-lg-0">
                                                    <div className="form-check form-switch form-check-inline" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="inlineswitch" />
                                                    </div>
                                                    <div className="form-check form-switch form-check-inline" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="inlineswitch1" />
                                                    </div>
                                                    <div className="form-check form-switch form-check-inline" dir="ltr">
                                                        <Input type="checkbox" className="form-check-input" id="inlineswitchdisabled" disabled />
                                                    </div>
                                                </div>
                                            </Col>

                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <WithoutLabels />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <PreviewCardHeader title="Radio Toggle Buttons" />
                                <CardBody>
                                    <p className="text-muted">Create button-like checkboxes and radio buttons by using <code>btn</code> styles rather than form-check-label class on the &lt;label&gt; elements. These toggle buttons can further be grouped in a <a href="https://getbootstrap.com/docs/5.1/components/button-group/">button group</a> if needed.</p>
                                    <div className="live-preview">
                                        <div className="hstack gap-2 flex-wrap">
                                            <Input type="radio" className="btn-check" name="options" id="option1" defaultChecked />
                                            <Label className="btn btn-secondary" for="option1">Checked</Label>

                                            <Input type="radio" className="btn-check" name="options" id="option2" />
                                            <Label className="btn btn-secondary" for="option2">Radio</Label>

                                            <Input type="radio" className="btn-check" name="options" id="option3" disabled />
                                            <Label className="btn btn-secondary" for="option3">Disabled</Label>

                                            <Input type="radio" className="btn-check" name="options" id="option4" />
                                            <Label className="btn btn-secondary" for="option4">Radio</Label>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <RadioToggleButtons />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <PreviewCardHeader title="Outlined Styles" />
                                <CardBody>
                                    <p className="text-muted">Different variants of <code>btn</code> attribute, such as the various outlined styles, are supported.</p>
                                    <div className="live-preview">
                                        <div className="hstack gap-2 flex-wrap">
                                            <Input type="checkbox" className="btn-check" id="btn-check-outlined" />
                                            <Label className="btn btn-outline-primary" for="btn-check-outlined">Single toggle</Label>

                                            <Input type="checkbox" className="btn-check" id="btn-check-2-outlined" defaultChecked />
                                            <Label className="btn btn-outline-secondary" for="btn-check-2-outlined">Checked</Label>

                                            <Input type="radio" className="btn-check" name="options-outlined" id="success-outlined" defaultChecked />
                                            <Label className="btn btn-outline-success" for="success-outlined">Checked success radio</Label>

                                            <Input type="radio" className="btn-check" name="options-outlined" id="danger-outlined" />
                                            <Label className="btn btn-outline-danger" for="danger-outlined">Danger radio</Label>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ "height": "275px" }}>
                                            <code>
                                                <OutlinedStyles />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>

        </React.Fragment>
    );
};

export default CheckBoxAndRadio;
