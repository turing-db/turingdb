import React from 'react';
import { Row, Col } from 'reactstrap';
import { Link } from 'react-router-dom';

//import Images
import logo_dark from "../../../../assets/images/logo-dark.png";
import img01 from "../../../../assets/images/products/img-1.png"
import img02 from "../../../../assets/images/products/img-2.png"
import img03 from "../../../../assets/images/products/img-3.png"

const RatingTemplate = () => {

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
                                <h5 className="mb-0 pb-1 text-decoration-underline">Rating and Review Email Template</h5>
                            </div>
                        </Col>
                       
                        <Col className="col-12">
                            <table className="body-wrap" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", width: "100%", backgroundColor: "transparent", margin: 0}}>
                                <tbody>
                                <tr style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}}>
                                    <td style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", verticalAlign: "top", margin: 0}} valign="top"></td>
                                    <td className="container" width="600" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", display: "block", maxWidth: "600px", clear: "both", margin: "0 auto"}} valign="top">
                                        <div className="content" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", maxWidth: "600px", display: "block", margin: "0 auto", padding: "20px"}}>
                                            <table className="main" width="100%" cellPadding="0" cellSpacing="0" itemProp="action" itemScope itemType="http://schema.org/ConfirmAction" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", borderRadius: "3px", margin: 0, border: "none"}}>
                                                <tbody>
                                                <tr style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", margin: 0}}>
                                                    <td className="content-wrap" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", color: "#495057",  fontSize: "14px", verticalAlign: "top", margin: 0, boxShadow: "0 3px 15px rgba(30,32,37,.06)", borderRadius: "7px", backgroundColor: "#fff", overflow: "hidden"}} valign="top">
                                                        <meta itemProp="name" content="Confirm Email" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "14px", margin: 0}} />
                                                        <div style={{padding: "20px", boxSizing: "border-box", textAlign: "center", backgroundImage: "linear-gradient(to right, #405189, #405189)"}}>
                                                            <h6 style={{fontFamily: "Roboto, sans-serif", margin: 0, fontSize: "15px", color: "#fff", textTransform: "uppercase"}}>FREE 1-2 day shipping on orders over $32</h6>
                                                        </div>
                                                        <div style={{padding: "20px", boxSizing: "border-box", textAlign: "center", borderBottom: "1px solid #e9ebec"}}>
                                                            <img src={logo_dark} alt="" height="23"/>
                                                        </div>
                                                        <div style={{padding: "20px", boxSizing: "border-box", textAlign: "center"}}>
                                                            <h5 style={{fontFamily: "Roboto, sans-serif", marginBottom: "10px", fontWeight: 500}}>What did you think of your recent purchase?</h5>
                                                            <div style={{color: "#f7b84b", fontSize: "17px"}}>
                                                                <i className="ri-star-fill"></i>
                                                                <i className="ri-star-fill"></i>
                                                                <i className="ri-star-fill"></i>
                                                                <i className="ri-star-fill"></i>
                                                                <i className="ri-star-fill"></i>
                                                            </div>
                                                            <p style={{fontSize: "14px", color: "#98a6ad", borderBottom: "1px solid #e9ebec", paddingBottom: "18px"}}>We'd love to hear how you and your pet enjoyed these products. Please leave a reviews so we can share it with other pet parents just like you.</p>
                                                        
                                                            <table style={{width:"100%", fontFamily: "Roboto, sans-serif"}}>
                                                                <thead>
                                                                    <tr style={{textAlign: "left"}}>
                                                                        <th style={{padding: "5px", width: "110px"}}>
                                                                            <img src={img03} alt="" height="80"/>
                                                                        </th>
                                                                        <th style={{padding: "5px"}}>
                                                                            <h6 style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", marginBottom: "2px", fontWeight: 500}}>350 ml Glass Grocery Container</h6>
                                                                            <p style={{color: "#878a99", fontWeight: "normal", marginBottom: "5px", lineHeight: 1.5, fontSize: "12px"}}>Category : Grocery</p>
                                                                            <Link to="#" itemProp="url" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "11.36px", fontWeight: "normal", color: "#FFF", textDecoration: "none", textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: "4px", textTransform: "capitalize", backgroundColor: "#0ab39c", margin: 0, borderColor: "#0ab39c", borderStyle: "solid", borderWidth: "1px", padding: "4px 8px"}} 
                                                                           onMouseOver={changeBackground} onMouseOut={bgColor}>Write a Review &#8594;</Link>
                                                                        </th>
                                                                    </tr>
                                                                    <tr style={{textAlign: "left"}}>
                                                                        <th style={{padding: "5px", width: "110px"}}>
                                                                            <img src={img02} alt="" height="80"/>
                                                                        </th>
                                                                        <th style={{padding: "5px"}}>
                                                                            <h6 style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", marginBottom: "2px", fontWeight: 500}}>Urban Ladder Pashe Chair</h6>
                                                                            <p style={{color: "#878a99", fontWeight: "normal", marginBottom: "5px", lineHeight: 1.5, fontSize: "12px"}}>Category : Furniture</p>
                                                                            <Link to="#" itemProp="url" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "11.36px", fontWeight: "normal", color: "#FFF", textDecoration: "none", textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: "4px", textTransform: "capitalize", backgroundColor: "#0ab39c", margin: 0, borderColor: "#0ab39c", borderStyle: "solid", borderWidth: "1px", padding: "4px 8px"}}
                                                                            onMouseOver={changeBackground} onMouseOut={bgColor}>Write a Review &#8594;</Link>
                                                                        </th>
                                                                    </tr>
                                                                    <tr style={{textAlign: "left"}}>
                                                                        <th style={{padding: "5px", width: "110px"}}>
                                                                            <img src={img01} alt="" height="80"/>
                                                                        </th>
                                                                        <th style={{padding: "5px"}}>
                                                                            <h6 style={{fontFamily: "Roboto, sans-serif", fontSize: "14px", marginBottom: "2px", fontWeight: 500}}>Half Sleeve Round Neck T-Shirts</h6>
                                                                            <p style={{color: "#878a99", fontWeight: "normal", marginBottom: "5px", lineHeight: 1.5, fontSize: "12px"}}>Category : Fashion</p>
                                                                            <Link to="#" itemProp="url" style={{fontFamily: "Roboto, sans-serif", boxSizing: "border-box", fontSize: "11.36px", fontWeight: "normal", color: "#FFF", textDecoration: "none", textAlign: "center", cursor: "pointer", display: "inline-block", borderRadius: "4px", textTransform: "capitalize", backgroundColor: "#0ab39c", margin: 0, borderColor: "#0ab39c", borderStyle: "solid", borderWidth: "1px", padding: "4px 8px"}} 
                                                                            onMouseOver={changeBackground} onMouseOut={bgColor}>Write a Review &#8594;</Link>
                                                                        </th>
                                                                    </tr>
                                                                </thead>
                                                            </table>
                                                        </div>
                                                        <div style={{padding: "20px", boxSizing: "border-box", textAlign: "center", backgroundColor: "rgba(240,101,72,.1)"}}>
                                                            <h6 style={{fontFamily: "Roboto, sansSerif", margin: 0, fontSize: "15px", textTransform: "uppercase", color: "#f06548"}}>Call us 24/7 at 1-2345-678-9123</h6>
                                                        </div>
                                                        <div style={{padding: "20px", boxSizing: "border-box", textAlign: "center"}}>
                                                            <p style={{fontFamily: "Roboto, sansSerif", marginBottom: "0px", fontWeight: 500, color: "#98a6ad"}}>Review text is a text that contains reviews, ratings or reviews of a work such as a film, drama, or a book.</p>
                                                        </div>
                                                    </td>
                                                </tr>
                                                </tbody>
                                            </table>
                                            <div style={{textAlign: "center", margin: "28px auto 0px auto"}}>
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

export default RatingTemplate;