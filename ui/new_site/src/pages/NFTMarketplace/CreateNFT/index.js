import React, { useState } from "react";
import { Card, CardBody, CardHeader, Col, Container, Form, Row } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";

// Import React FilePond
import { FilePond, registerPlugin } from 'react-filepond';
// Import FilePond styles
import 'filepond/dist/filepond.min.css';
import FilePondPluginImageExifOrientation from 'filepond-plugin-image-exif-orientation';
import FilePondPluginImagePreview from 'filepond-plugin-image-preview';
import 'filepond-plugin-image-preview/dist/filepond-plugin-image-preview.css';


// Register the plugins
registerPlugin(FilePondPluginImageExifOrientation, FilePondPluginImagePreview);

const CreateNFT = () => {
    document.title = "Create NFT | Velzon - React Admin & Dashboard Template";

    const [files, setFiles] = useState([]);

    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Create NFT" pageTitle="NFT Marketplace" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h5 className="card-title mb-0">Create New File</h5>
                                </CardHeader>
                                <CardBody>
                                    <Form>
                                        <Row className="g-3">
                                            <Col lg={12}>
                                                <h5 className="fw-semibold mb-3">Image, Video, Audio, or 3D Model</h5>
                                                <FilePond
                                                    files={files}
                                                    onupdatefiles={setFiles}
                                                    allowMultiple={true}
                                                    maxFiles={3}
                                                    name="filepond"
                                                    className="filepond filepond-input-multiple"
                                                />
                                            </Col>
                                            <Col lg={12}>
                                                <label htmlFor="productName" className="form-label">Product Name</label>
                                                <input type="text" className="form-control" id="productName" placeholder="Enter product name" />
                                            </Col>
                                            <Col lg={12}>
                                                <label htmlFor="discription" className="form-label">Discription</label>
                                                <textarea className="form-control" id="discription" rows="3" placeholder="Enter discription"></textarea>
                                            </Col>
                                            <Col lg={12}>
                                                <label htmlFor="externalLink" className="form-label">External link</label>
                                                <input type="text" className="form-control" id="externalLink" placeholder="Enter external link" />
                                            </Col>
                                            <Col lg={4}>
                                                <label htmlFor="itemPrice" className="form-label">Item Price</label>
                                                <input type="text" className="form-control" id="itemPrice" placeholder="Price" />
                                            </Col>
                                            <Col lg={4}>
                                                <label htmlFor="size" className="form-label">Size</label>
                                                <input type="text" className="form-control" id="size" placeholder="Size" />
                                            </Col>
                                            <Col lg={4}>
                                                <label htmlFor="properties" className="form-label">Properties</label>
                                                <input type="text" className="form-control" id="properties" placeholder="Properties" />
                                            </Col>
                                            <Col lg={12}>
                                                <div>
                                                    <div className="form-check form-check-inline">
                                                        <input className="form-check-input" type="checkbox" id="inlineCheckbox1" value="option1" />
                                                        <label className="form-check-label" htmlFor="inlineCheckbox1">Put on sale</label>
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <input className="form-check-input" type="checkbox" id="inlineCheckbox2" value="option2" />
                                                        <label className="form-check-label" htmlFor="inlineCheckbox2">Instant sale price</label>
                                                    </div>
                                                    <div className="form-check form-check-inline">
                                                        <input className="form-check-input" type="checkbox" id="inlineCheckbox3" value="option3" />
                                                        <label className="form-check-label" htmlFor="inlineCheckbox3">Unlock one purchased</label>
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col lg={12}>
                                                <div className="text-end">
                                                    <button className="btn btn-primary">Create Item</button>
                                                </div>
                                            </Col>
                                        </Row>
                                    </Form>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default CreateNFT;
