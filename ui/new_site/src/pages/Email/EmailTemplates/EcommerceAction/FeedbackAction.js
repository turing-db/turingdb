import React from 'react';
import { Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

const FeedbackAction = () => {
    return (
        <React.Fragment>
            <Row>
                <Col lg={12}>
                    <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                        <h5 className="mb-0 pb-1 text-decoration-underline">Feedback Action</h5>
                    </div>
                </Col>

                <Col className="col-12">
                    <table className="body-wrap" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", width: "100%", backgroundColor: "transparent", margin: 0 }}>
                        <tbody>
                            <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                <td style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0 }} valign="top"></td>
                                <td className="container" width="600" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", display: "block !important", maxWidth: "600px !important", clear: "both !important", margin: "0 auto" }} valign="top">
                                    <div className="content" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", maxWidth: "600px", display: "block", margin: "0 auto", padding: "20px" }}>
                                        <table className="main" width="100%" cellPadding="0" cellSpacing="0" itemProp="action" itemScope itemType="http://schema.org/ConfirmAction" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", borderRadius: "3px", margin: 0, border: "none" }}>
                                            <tbody>
                                                <tr style={{ fontFamily: "Roboto, sans-serif", fontSize: "14px", margin: 0 }}>
                                                    <td className="content-wrap" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", color: "#495057", fontSize: "14px", verticalAlign: "top", margin: 0, padding: "30px", boxShadow: "0 3px 15px rgba(30,32,37,.06)", borderRadius: "7px", backgroundColor: "#fff" }} valign="top">
                                                        <meta itemProp="name" content="Confirm Email" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }} />
                                                        <table width="100%" cellPadding="0" cellSpacing="0" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                            <tbody>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "22px", verticalAlign: "top", margin: 0, padding: "0 0 3px", textAlign: "center" }} valign="top">
                                                                        <div style={{ color: "#f7b84b" }}>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                            <i className="ri-star-fill"></i>
                                                                        </div>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: 0 }} valign="top">
                                                                        <h3 style={{ fontFamily: "Roboto, sans-serif", marginBottom: "18px", textlign: "center", lineHeight: 1.5 }}>Good design & good support. Go for it</h3>
                                                                    </td>
                                                                </tr>
                                                                <tr style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0 }}>
                                                                    <td className="content-block" style={{ fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "15px", verticalAlign: "top", margin: 0, padding: "0 0 0px" }} valign="top">
                                                                        <p style={{ fontFamily: "Roboto, sans-serif", marginBottom: "8px", color: "#878a99", lineHeight: 1.5 }}>Dear valuable Customer,</p>
                                                                        <p style={{ fontFamily: "Roboto, sans-serif", marginBottom: "8px", color: "#878a99", lineHeight: 1.5 }}>Thank you so much for providing 5 stars review It really means a lot to us. Your review motivates us to work even hard and provide top-notch features, support, and updates. You can share more details via the contact us so we can provide the best from our side in future updates. We are going to release the Vue and React version asap.</p>
                                                                        <p style={{ fontFamily: "Roboto, sans-serif", marginBottom: "8px", color: "#878a99", lineHeight: 1.5 }}>Feel free to get in touch with us anytime via support from the link below. <Link to="https://themeforest.net/item/velzon-aspnet-core-admin-dashboard-template/36077495/support" target="_blank">https://themeforest.net/item/velzon-aspnet-core-admin-dashboard-template/36077495/support</Link></p>
                                                                        <h6 style={{ fontFamily: "Roboto, sans-serif", fontSize: "14px", marginBottom: "0px", textAlign: "end" }}>Thank you!</h6>
                                                                    </td>
                                                                </tr>
                                                            </tbody>
                                                        </table>
                                                    </td>
                                                </tr>
                                            </tbody>
                                        </table>
                                        <div style={{ textAlign: "center", margin: "28px auto 0px auto" }}>
                                            <p style={{ fontFamily: "Roboto sans-serif", fontSize: "14px", color: "#98a6ad", margin: "0px" }}>2022 Velzon. Design & Develop by Themesbrand</p>
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

export default FeedbackAction;