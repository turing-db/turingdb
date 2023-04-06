import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';

//Import Breadcrumb
import BreadCrumb from '../../../Components/Common/BreadCrumb';

//Import maps
import SimpleMap from "./MapsLeaflet/SimpleMap";
import MapWithPopup from "./MapsLeaflet/MapWithPopup";
import MapVectorLayers from "./MapsLeaflet/MapVectorLayers";
import MapMarkerCustomIcons from "./MapsLeaflet/MapMarkerCustomIcons";
import LayerGroup from "./MapsLeaflet/LayerGroup";
import MapLayerControl from "./MapsLeaflet/MapLayerControl";

const LeafletMaps = () => {
  document.title="Leaflet Maps | Velzon - React Admin & Dashboard Template";

    return (
        <React.Fragment>            
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Leaflet" pageTitle="Maps" />
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Example</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="leaflet-map" className="leaflet-map">
                                        <SimpleMap />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Markers, Circles and Polygons</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="leaflet-map-marker" className="leaflet-map">
                                        <MapVectorLayers />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Working with Popups</h4>
                                </CardHeader>
                                <CardBody>
                                    <div id="leaflet-map-popup" className="leaflet-map">
                                        <MapWithPopup />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Markers with Custom Icons</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="leaflet-map-custom-icons" className="leaflet-map">
                                        <MapMarkerCustomIcons />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Interactive Choropleth Map</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="leaflet-map-interactive-map" className="leaflet-map">
                                        <LayerGroup />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Layer Groups and Layers Control</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="leaflet-map-group-control" className="leaflet-map">
                                        <MapLayerControl />
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

export default LeafletMaps;