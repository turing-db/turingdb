import React from 'react';
import { Container, Row, Col, Card, CardBody, CardHeader } from 'reactstrap';
import { GoogleApiWrapper, Map, Marker } from "google-maps-react";

//Import Breadcrumb
import BreadCrumb from '../../../Components/Common/BreadCrumb';

const mapStyles = {
    width: '100%',
    height: '100%',
};

const LoadingContainer = () => <div>Loading...</div>

const GoogleMaps = (props) => {
  document.title="Google Maps | Velzon - React Admin & Dashboard Template";

    return (
        <React.Fragment>                
                <div className="page-content">
                    <Container fluid>
                    
                    <BreadCrumb title="Google Maps" pageTitle="Maps" />

                        <Row>
                            <Col lg={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Markers</h4>
                                    </CardHeader>
                                    <CardBody>
                                        <div id="gmaps-markers" className="gmaps" style={{ position: "relative" }}>
                                            <Map
                                                google={props.google}
                                                zoom={8}
                                                style={mapStyles}
                                                initialCenter={{ lat: 34.134117, lng: -118.321495 }}
                                            >
                                                <Marker position={{ lat: 48.00, lng: -122.00 }} />
                                                <Marker position={{ lat: 34.134117, lng: -118.321495 }} />
                                                <Marker position={{ lat: 32.995049, lng: -111.536324 }} />
                                                <Marker position={{ lat: 37.383064, lng: -109.071236 }} />
                                                <Marker position={{ lat: 39.877586, lng: -79.640617 }} />
                                            </Map>
                                            </div>
                                    </CardBody>
                                </Card>
                            </Col>
                            <Col lg={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Overlays</h4>
                                    </CardHeader>
                                    <CardBody >
                                        <div id="gmaps-overlay" className="gmaps" style={{ position: "relative" }}>
                                        <Map
                                                google={props.google}
                                                zoom={8}
                                                style={mapStyles}
                                                initialCenter={{ lat: 54.5260, lng: 15.2551 }}
                                            />
                                            </div>
                                    </CardBody>
                                </Card>
                            </Col>                            
                        </Row>                    

                        <Row>
                            <Col lg={6}>
                                <Card >
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Street View Panoramas</h4>
                                    </CardHeader>
                                    <CardBody>
                                        <div id="panorama" className="gmaps-panaroma" style={{ position: "relative" }}>
                                        <Map
                                                google={props.google}
                                                zoom={8}
                                                style={mapStyles}
                                                initialCenter={{ lat: 8.7832, lng: 34.5085 }}
                                            />
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>                           

                            <Col lg={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Map Types</h4>
                                    </CardHeader>

                                    <div className="card-body">
                                        <div id="gmaps-types" className="gmaps" style={{ position: "relative" }}>
                                        <Map
                                            google={props.google}
                                            zoom={8}
                                            style={mapStyles}
                                            initialCenter={{ lat: 19.0760, lng: 72.8777 }}
                                        />
                                        </div>
                                    </div>
                                </Card>
                            </Col>                            
                        </Row>                     
                    </Container>                    
                </div>
        </React.Fragment>
    );
}

export default (
    GoogleApiWrapper({
        apiKey: "AIzaSyAbvyBxmMbFhrzP9Z8moyYr6dCr-pzjhBE",
        LoadingContainer: LoadingContainer,
        v: "3",
    })(GoogleMaps)
)




