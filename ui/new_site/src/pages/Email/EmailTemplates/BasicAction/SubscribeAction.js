import React from 'react';
import { Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

//import Images
import logo_dark from "../../../../assets/images/logo-dark.png";

const SubscribeAction = () => {
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
                        <h5 className="mb-0 pb-1 text-decoration-underline">Subscribe Action</h5>
                    </div>
                </Col>

                <Col className="col-12">
                    <table className="body-wrap"
                        style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", 
                    width: "100%", backgroundColor: "transparent", margin: 0,}}
                    >
                        <tbody>
                            <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                <td style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0}} valign="top"></td>
                                <td className="container" width="600" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", display: "block !important", maxWidth: "600px !important", clear: "both !important", margin: "0 auto"}} valign="top">
                                    <div className="content" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", maxWidth: "600px", display: "block", margin: "0 auto", padding: "20px"}}>
                                        <table className="main" width="100%" cellPadding="0" cellSpacing="0" itemProp="action" itemScope itemType="http://schema.org/ConfirmAction" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", borderRadius: "3px", margin: 0, border: "none"}}>
                                            <tbody>
                                                <tr style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", margin: 0}}>
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
                                                                    <td className="content-block" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", lineHeight: "1.5", fontSize: "24px", verticalAlign: "top", margin: 0, padding: "0 0 10px", textAlign: "center", fontWeight: 500}} valign="top">
                                                                        Please confirm subscription
                                                                    </td>
                                                                </tr>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                    <td className="content-block" style={{fontFamily: "Roboto, sans-serif", color: "#878a99", lineHeight:" 1.5", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 24px", textAlign: "center"}} valign="top">
                                                                        Since yesterday, I've been receiving thousands of emails, asking me to confirm the subscription.
                                                                    </td>
                                                                </tr>
                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                                                     <td className="content-block" itemProp="handler" itemScope itemType="http://schema.org/HttpActionHandler" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "0 0 24px", textAlign: "center"}} valign="top">
                                                                        <Link to="#" itemProp="url" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: ".8125rem", fontWeight: 400, color: "#FFF", textDecoration: "none", textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: ".25rem", textTransform: "capitalize", backgroundColor: "#0ab39c", margin: 0, borderColor: "#0ab39c", borderStyle: "solid", borderWidth: "1px", padding: ".5rem .9rem"}} 
                                                                        onMouseOver={changeBackground} onMouseOut={bgColor}
                                                                        >
                                                                        Yes, subscribe me
                                                                        </Link>
                                                                    </td>
                                                                </tr>

                                                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0, borderTop: "1px solid #e9ebec"}}>
                                                                    <td className="content-block" style={{color: "#878a99", textAlign: "center", fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0, padding: 0, paddingTop: "15px"}} valign="top">
                                                                        If you received this email by mistake, simply delete it. You won't be subscribed if you don't click the confirmation link above.
                                                                    </td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                        <div style={{textAlign: "center", margin: "0px auto"}}>
                                            <ul style={{listStyle: "none", display: "flex", justifyContent: "space-evenly", paddingTop: "25px", marginBottom: "20px", paddingLeft: "0px", fontFamily: "Roboto sans-serif"}}>
                                                <li>
                                                    <Link to="/#" style={{color: "#495057"}}>Help Center</Link>
                                                </li>
                                                <li>
                                                    <Link to="/#" style={{color: "#495057"}}>Support 24/7</Link>
                                                </li>
                                                <li>
                                                    <Link to="/#" style={{color: "#495057"}}>Account</Link>
                                                </li>
                                            </ul>
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

export default SubscribeAction;