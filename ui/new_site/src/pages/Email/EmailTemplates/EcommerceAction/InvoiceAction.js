import React from 'react';
import { Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

function InvoiceAction() {
    return (
        <React.Fragment>
            <Row>
                <Col lg={12}>
                    <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                        <h5 className="mb-0 pb-1 text-decoration-underline">Invoice Action</h5>
                    </div>
                </Col>

                <Col className="col-12">
                    <table className="body-wrap" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", width: "100%", backgroundColor: "transparent", margin: 0 }}>
                        <tbody>
                            <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                <td style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0 }} valign="top"></td>
                                <td className="container" width="600" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", display: "block", maxWidth: "600px", clear: "both", margin: "0 auto" }} valign="top">
                                    <div className="content" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", maxWidth: "600px", display: "block", margin: "0 auto", padding: "20px" }}>
                                        <table className="main" width="100%" cellPadding="0" cellSpacing="0" itemProp="action" itemScope itemType="http://schema.org/ConfirmAction" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", borderRadius: "3px", margin: 0, border: "none" }}>
                                            <tbody>
                                                <tr style={{ fontFamily: "Roboto, sans-serif", fontSize: "14px", margin: 0 }}>
                                                    <td className="content-wrap" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", color: "#495057", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "30px", boxShadow: "0 3px 15px rgba(30,32,37,.06)", borderRadius: "7px", backgroundColor: "#fff" }} valign="top">
                                                        <meta itemProp="name" content="Confirm Email" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }} />
                                                        <table width="100%" cellPadding="0" cellSpacing="0" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                            <tbody>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "24px", verticalAlign: "top", margin: 0, padding: "0 0 10px", textAlign: "center" }} valign="top">
                                                                        <h4 style={{ fontFamily: "Roboto, sans-serif", marginBottom: "10px", fontWeight: 600 }}>Your order has been placed</h4>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 12px" }} valign="top">
                                                                        <h5 style={{ fontFamily: "Roboto, sans-serif", marginBottom: "3px" }}>Hey, Anna Adame</h5>
                                                                        <p style={{ fontFamily: "Roboto, sans-serif", marginBottom: "8px", color: "#878a99" }}>Your order has been confirmed and will be shipping soon.</p>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 18px" }} valign="top">
                                                                        <table style={{ width: "100%" }}>
                                                                            <thead>
                                                                                <tr style={{ textAlign: "left" }}>
                                                                                    <th style={{ padding: "5px" }}>
                                                                                        <p style={{ color: "#878a99", fontSize: "13px", marginBottom: "2px", fontWeight: 400 }}>Order Number</p>
                                                                                        <span>VZ14524742541</span>
                                                                                    </th>
                                                                                    <th style={{ padding: "5px" }}>
                                                                                        <p style={{ color: "#878a99", fontSize: "13px", marginBottom: "2px", fontWeight: 400 }}>Order Date</p>
                                                                                        <span>05 April, 2022</span>
                                                                                    </th>
                                                                                    <th style={{ padding: "5px" }}>
                                                                                        <p style={{ color: "#878a99", fontSize: "13px", marginBottom: "2px", fontWeight: 400 }}>Payment Method</p>
                                                                                        <span>Viss - 4622</span>
                                                                                    </th>
                                                                                </tr>
                                                                            </thead>
                                                                        </table>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 12px" }} valign="top">
                                                                        <h6 style={{ fontFamily: "Roboto, sans-serif", fontSize: "15px", textDecorationLine: "underline", marginBottom: "15px" }}>Her'e what you ordered:</h6>
                                                                        <table style={{ width: "100%" }} cellSpacing="0" cellPadding="0">
                                                                            <thead style={{ textAlign: "left" }}>
                                                                                <tr>
                                                                                    <th style={{ padding: "8px", borderBottom: "1px solid #e9ebec" }}>Product Details</th>
                                                                                    <th style={{ padding: "8px", borderBottom: "1px solid #e9ebec" }}>Quantity</th>
                                                                                    <th style={{ padding: "8px", borderBottom: "1px solid #e9ebec" }}>Amount</th>
                                                                                </tr>
                                                                            </thead>
                                                                            <tbody>
                                                                                <tr>
                                                                                    <td style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        <h6 style={{ marginBottom: "2px", fontSize: "14px" }}>Sweatshirt for Men (Pink)</h6>
                                                                                        <p style={{ marginBottom: "2px", fontSize: "13px", color: "#878a99" }}>Graphic Print Men & Women Sweatshirt</p>
                                                                                    </td>
                                                                                    <td style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        02
                                                                                    </td>
                                                                                    <td style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        $239.98
                                                                                    </td>
                                                                                </tr>
                                                                                <tr>
                                                                                    <td style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        <h6 style={{ marginBottom: "2px", fontSize: "14px" }}>Noise NoiseFit Endure Smart Watch</h6>
                                                                                        <p style={{ marginBottom: "2px", fontSize: "13px", color: "#878a99" }}>32.5mm (1.28 Inch) TFT Color Touch Display</p>
                                                                                    </td>
                                                                                    <td style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        01
                                                                                    </td>
                                                                                    <td style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        $94.99
                                                                                    </td>
                                                                                </tr>
                                                                                <tr>
                                                                                    <td colSpan="2" style={{ padding: "8px", fontSize: "13px", textAlign: "end", borderTop: "1px solid #e9ebec" }}>
                                                                                        Subtotal
                                                                                    </td>
                                                                                    <th style={{ padding: "8px", fontSize: "13px", borderTop: "1px solid #e9ebec" }}>
                                                                                        $334.97
                                                                                    </th>
                                                                                </tr>
                                                                                <tr>
                                                                                    <td colSpan="2" style={{ padding: "8px", fontSize: "13px", textAlign: "end" }}>
                                                                                        Shipping Charge
                                                                                    </td>
                                                                                    <th style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        $9.50
                                                                                    </th>
                                                                                </tr>
                                                                                <tr>
                                                                                    <td colSpan="2" style={{ padding: "8px", fontSize: "13px", textAlign: "end" }}>
                                                                                        Taxs
                                                                                    </td>
                                                                                    <th style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        $15.26
                                                                                    </th>
                                                                                </tr>
                                                                                <tr>
                                                                                    <td colSpan="2" style={{ padding: "8px", fontSize: "13px", textAlign: "end" }}>
                                                                                        Discount
                                                                                    </td>
                                                                                    <th style={{ padding: "8px", fontSize: "13px" }}>
                                                                                        $20.78
                                                                                    </th>
                                                                                </tr>
                                                                                <tr>
                                                                                    <td colSpan="2" style={{ padding: "8px", fontSize: "13px", textAlign: "end", borderTop: "1px solid #e9ebec" }}>
                                                                                        Total Amount
                                                                                    </td>
                                                                                    <th style={{ padding: "8px", fontSize: "13px", borderTop: "1px solid #e9ebec" }}>
                                                                                        $338.95
                                                                                    </th>
                                                                                </tr>
                                                                            </tbody>
                                                                        </table>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 0px" }} valign="top">
                                                                        <p style={{ fontFamily: "Roboto, sans-serif", marginBottom: "8px", color: "#878a99" }}>Wl'll send you shipping confirmation when your item(s) are on the way! We appreciate your business, and hope you enjoy your purchase.</p>
                                                                        <h6 style={{ fontFamily: "Roboto, sans-serif", fontSize: "14px", marginBottom: "0px", textAlign: "end" }}>Thank you!</h6>
                                                                    </td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>

                                        <div style={{ marginTop: "32px", textAlign: "center" }}>
                                            <Link to="#" itemProp="url" className="me-1" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: ".8125rem", color: "#FFF", textDecoration: "none", fontWeight: 400, textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: ".25rem", textTransform: "capitalize", backgroundColor: "#405189", margin: 0, borderColor: "#405189", borderStyle: "solid", borderWidth: "1px", padding: ".5rem .9rem" }}>Download</Link>
                                            <Link to="#" itemProp="url" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: ".8125rem", color: "#FFF", textDecoration: "none", fontWeight: 400, textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: ".25rem", textTransform: "capitalize", backgroundColor: "#0ab39c", margin: 0, borderColor: "#0ab39c", borderStyle: "solid", borderWidth: "1px", padding: ".5rem .9rem" }}>Back to Shop</Link>
                                        </div>
                                        <div style={{ textAlign: "center", margin: "28px auto 0px auto" }}>
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
}

export default InvoiceAction;