import React from "react";
import { Card, CardBody, Col, Row, CardHeader, Form, Container } from "reactstrap";
import UiContent from "../../../Components/Common/UiContent";

import { CKEditor } from "@ckeditor/ckeditor5-react";
import ClassicEditor from "@ckeditor/ckeditor5-build-classic";
import BreadCrumb from "../../../Components/Common/BreadCrumb";

import { useQuill } from "react-quilljs";
import "quill/dist/quill.snow.css";

const FormEditor = () => {
  document.title="Editors | Velzon - React Admin & Dashboard Template";
  
  const {  quillRef } = useQuill();

  return (
    <React.Fragment>
      <UiContent />
      <div className="page-content">        
        <Container fluid>
          <BreadCrumb title="Editors" pageTitle="Forms" />

          <Row>
            <Col lg={12}>
              <Card>
                <CardHeader className="align-items-center d-flex">
                  <h4 className="card-title mb-0">Ckeditor Classic Editor</h4>
                </CardHeader>
                <CardBody>
                  <Form method="post">
                    <CKEditor
                      editor={ClassicEditor}
                      data="<p>Hello from CKEditor 5!</p>"
                      onReady={(editor) => {
                        // You can store the "editor" and use when it is needed.
                        
                      }}
                      onChange={(editor) => {
                        editor.getData();
                      }}
                    />
                  </Form>
                </CardBody>
              </Card>
            </Col>
          </Row>

          <Row className="mt-2">
            <Col lg={12}>
              <div className="justify-content-between d-flex align-items-center mb-3">
                <h5 className="mb-0 pb-1 text-decoration-underline">
                  Quilljs Editor
                </h5>
              </div>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Snow Editor</h4>
                </CardHeader>
                <CardBody>                  
                  <div className="snow-editor" style={{ height: 300 }}>
                    <div ref={quillRef} />
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

export default FormEditor;
