import React from 'react';
import { Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

//import Images
import logo_dark from "../../../../assets/images/logo-dark.png";

const EmailVerifyAction = () => {
    return (
        <React.Fragment>
            <Row>
                <Col lg={12}>
                    <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                        <h5 className="mb-0 pb-1 text-decoration-underline">Email Verify Action</h5>
                    </div>
                </Col>

                <Col className="col-12">
                    <table className="body-wrap" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", width: "100%", backgroundColor: "transparent", margin: 0}}>
                        <tbody>
                            <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                <td style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0}} valign="top"></td>
                                <td className="container" width="600" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", display: "block !important", maxWidth: "600px !important", clear: "both !important", margin: "0 auto"}} valign="top">
                                    <div className="content" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", maxWidth: "600px", display: "block", margin: "0 auto", padding: "20px"}}>
                                        <table className="main" width="100%" cellPadding="0" cellSpacing="0" itemProp="action" itemScope itemType="http://schema.org/ConfirmAction" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", borderRadius: "3px", margin: 0, border: "none"}}>
                                            <tbody>
                                                <tr style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", margin: 0,}}>
                                                    <td className="content-wrap" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", color: "#495057", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "30px", boxShadow: "0 3px 15px rgba(30,32,37,.06)", borderRadius: "7px", backgroundColor: "#fff"}} valign="top">
                                                        <meta itemProp="name" content="Confirm Email" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}} />
                                                        <table width="100%" cellPadding="0" cellSpacing="0" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                            <tbody>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                    <td className="content-block" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "0 0 20px"}} valign="top">
                                                                        <div style={{textAlign: "center", marginBottom: "15px"}}>
                                                                            <img src={logo_dark} alt="" height="23" />
                                                                        </div>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                    <td className="content-block" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "24px", verticalAlign: "top", margin: 0, padding: "0 0 10px",  textAlign: "center"}} valign="top">
                                                                        <h4 style={{fontFamily: "Roboto, sans-serif", fontWeight: 500}}>Please Verify your email</h4>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                    <td className="content-block" style={{fontFamily: "Roboto, sans-serif", color: "#878a99", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 26px", textAlign: "center"}} valign="top">
                                                                        Yes, we know
                                                                        <p style={{marginBottom: "13px"}}>An email to verify an email.</p>
                                                                        <p style={{marginBottom: 0}}>Please validate your email address in order to get started using product.</p>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                    <td className="content-block" itemProp="handler" itemScope itemType="http://schema.org/HttpActionHandler" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "0 0 22px", textAlign: "center"}} valign="top">
                                                                        <Link to="#" itemProp="url" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: ".8125rem", color: "#FFF", textDecoration: "none", fontWeight: 400, textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: ".25rem", textTransform: "capitalize", backgroundColor: "#405189", margin: 0, borderColor: "#405189", borderStyle: "solid", borderWidth: "1px", padding: ".5rem .9rem"}}>Verify Your Email</Link>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                    <td className="content-block" style={{color: "#878a99", textAlign: "center", fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: 0, paddingTop: "5px"}} valign="top">
                                                                        <p style={{marginBottom: "10px"}}>Or verify using this link: </p>
                                                                        <Link to="https://themesbrand.com/velzon/" target="_blank">https://themesbrand.com/velzon/</Link>
                                                                    </td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                        <div style={{textAlign: "center", margin: "25px auto 0px auto", fontFamily: "Roboto sans-serif"}}>
                                            <h4 style={{fontWeight: 500, lineHeight: "1.5", fontFamily: "Roboto sans-serif"}}>Need Help ?</h4>
                                            <p style={{color: "#878a99", lineHeight: 1.5}}>Please send and feedback or bug info to <Link to="" style={{fontWeight: 500}}>info@velzon.com</Link></p>
                                            <p style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", color: "#98a6ad", margin: "0px"}}>2022 Velzon. Design & Develop by Themesbrand</p>
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
}

export default EmailVerifyAction;