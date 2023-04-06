import React from 'react';
import { Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

//import Images
import logo_dark from "../../../../assets/images/logo-dark.png";
import avatar03 from "../../../../assets/images/users/avatar-3.jpg";

const BasicAction = () => {

    function changeBackground(e) {
        e.target.style.background = "#099885";
    }
    function bgColor(e) {
        e.target.style.background = "#0ab39c";
    }
    return (
        <React.Fragment>
            <Row>
                <Col lg={12}>
                    <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                        <h5 className="mb-0 pb-1 text-decoration-underline">Basic Action</h5>
                    </div>
                </Col>
                <Col className="col-12">
                    <table className="body-wrap"
                        style={{
                            fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px",
                            width: "100%", backgroundColor: "transparent", margin: 0
                        }}>
                        <tbody>
                            <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                <td style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0 }} valign="top"></td>
                                <td className="container" width="600" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", display: "block", maxWidth: "600px", clear: "both", margin: "0, auto" }} valign="top">
                                    <div className="content" style={{ fontFamily: "Roboto sans-serif", boxSizing: "border-box", fontSize: "14px", maxWidth: "600px", display: "block", margin: "0 auto", padding: "20px" }}>
                                        <table className="main" width="100%" cellPadding="0" cellSpacing="0" itemProp="action" itemScope itemType="http://schema.org/ConfirmAction" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", borderRadius: "3px", margin: 0, border: "none" }}>
                                            <tbody>
                                                <tr style={{ fontFamily: "Roboto, sans-serif", fontSize: "14px", margin: 0 }}>
                                                    <td className="content-wrap" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", color: "#495057", fontSize: "14px", verticalAlign: "top", margin: "0", padding: "30px", boxShadow: "0 3px 15px rgba(30,32,37,.06)", borderRadius: "7px", backgroundColor: "#fff" }} valign="top">
                                                        <meta itemProp="name" content="Confirm Email" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }} />
                                                        <table width="100%" cellPadding="0" cellSpacing="0" style={{ fontFamily: "Roboto sansSerif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                            <tbody>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "borderBox", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "0 0 20px" }} valign="top">
                                                                        <div style={{ marginBottom: "15px" }}>
                                                                            <img src={logo_dark} alt="" height="23" />
                                                                        </div>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "20px", lineHeight: "1.5", fontWeight: "500", verticalAlign: "top", margin: 0, padding: "0 0 10px" }} valign="top">
                                                                        Hey, Anna Adame
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", color: "#878a99", boxSizing: "border-box", lineHeight: "1.5", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 10px" }} valign="top">
                                                                        I'm writing to let you know that I can't attend className for two weeks as I've had an accident and hurt my back. I'm afraid I also need to ask for an extension for next week's essay.
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", color: "#878a99", boxSizing: "border-box", lineHeight: "1.5", fontSize: "15px", verticalAlign: " top", margin: 0, padding: "0 0 24px" }} valign="top">
                                                                        Until then, my friend is going to record the lectures so I can listen to them at home. I will try to catch up with all the reading too. However, I don't think I can submit the essay next week. Is it possible to have an extension?
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" itemProp="handler" itemScope itemType="http://schema.org/HttpActionHandler" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "0 0 24px" }} valign="top">
                                                                        <Link to="#" itemProp="url" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: ".8125rem", fontWeight: "400", color: "#FFF", textDecoration: "none", textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: ".25rem", textTransform: "capitalize", backgroundColor: "#0ab39c", margin: 0, borderColor: "#0ab39c", borderStyle: "solid", borderWidth: "1px", padding: "8px 15px" }}
                                                                            onMouseOver={changeBackground} onMouseOut={bgColor}
                                                                        >
                                                                            Read More &#8594;</Link>
                                                                    </td>
                                                                </tr>

                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0, borderTop: "1px solid #e9ebec" }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: 0, paddingTop: "15px" }} valign="top">
                                                                        <div style={{ display: "flex", alignItems: "center" }}>
                                                                            <img src={avatar03} alt="" height="35" width="35" style={{ borderRadius: "50px" }} />
                                                                            <div style={{ marginLeft: "8px" }}>
                                                                                <span style={{ fontWeight: "600" }}>Luis Rocha</span>
                                                                                <p style={{ fontSize: "13px", marginBottom: "0px", marginTop: "3px", color: "#878a99" }}>Creative Director</p>
                                                                            </div>
                                                                        </div>
                                                                    </td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                        <div style={{ textAlign: "center", margin: "0px auto" }}>
                                            <ul style={{ listStyle: "none", display: "flex", justifyContent: "space-evenly", paddingTop: "25px", paddingLeft: "0px", marginBottom: "20px", fontFamily: "Roboto, sans-serif" }}>
                                                <li>
                                                    <Link to="/#" style={{ color: "#495057" }}>Help Center</Link>
                                                </li>
                                                <li>
                                                    <Link to="/#" style={{ color: "#495057" }}>Support 24/7</Link>
                                                </li>
                                                <li>
                                                    <Link to="/#" style={{ color: "#495057" }}>Account</Link>
                                                </li>
                                            </ul>
                                            <p style={{ fontFamily: "Roboto, sans-serif", fontSize: "14px", color: "#98a6ad", margin: "0px" }}>2022 Velzon. Design & Develop by Themesbrand</p>
                                        </div>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </Col>
            </Row>
        </React.Fragment>
    );
};

export default BasicAction;