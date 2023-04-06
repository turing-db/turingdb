import React, { useState } from 'react';
import { Col, Dropdown, DropdownMenu, DropdownToggle, Row } from 'reactstrap';
import { Link } from 'react-router-dom';

//SimpleBar
import SimpleBar from "simplebar-react";

//import images
import image1 from "../../assets/images/products/img-1.png";
import image2 from "../../assets/images/products/img-2.png";
import image3 from "../../assets/images/products/img-3.png";
import image6 from "../../assets/images/products/img-6.png";
import image5 from "../../assets/images/products/img-5.png";

const MyCartDropdown = () => {
    const cartData = [
        { id: 1, img: image1, product: "Branded T-Shirts", quantity: 10, price: 32 },
        { id: 2, img: image2, product: "Bentwood Chair", quantity: 5, price: 18 },
        { id: 3, img: image3, product: "Borosil Paper Cup", quantity: 3, price: 250 },
        { id: 4, img: image6, product: "Gray Styled T-Shirt", quantity: 1, price: 1250 },
        { id: 5, img: image5, product: "Stillbird Helmet", quantity: 2, price: 495 },
    ];

    const [isCartDropdown, setIsCartDropdown] = useState(false);

    const [cartItem, setCartItem] = useState(cartData.length);

    const toggleCartDropdown = () => {
        setIsCartDropdown(!isCartDropdown);
        setCartItem(cartData.length);
    };

    const removeItem = (ele) => {
        var price = ele.closest(".dropdown-item-cart").querySelector('.cart-item-price').innerHTML;
        var subTotal = document.getElementById('cart-item-total').innerHTML;
        document.getElementById('cart-item-total').innerHTML = subTotal - price;

        ele.closest(".dropdown-item-cart").remove();
        const element = document.querySelectorAll(".dropdown-item-cart").length;
        const ecart = document.getElementById("empty-cart");

        if (element === 0) {
            ecart.style.display = 'block';
        } else {
            ecart.style.display = 'none';
        }

        setCartItem(element);
    };
    return (
        <React.Fragment>
            <Dropdown isOpen={isCartDropdown} toggle={toggleCartDropdown} className="topbar-head-dropdown ms-1 header-item">
                <DropdownToggle type="button" tag="button" className="btn btn-icon btn-topbar btn-ghost-secondary rounded-circle">
                    <i className='bx bx-shopping-bag fs-22'></i>
                    <span
                        className="position-absolute topbar-badge fs-10 translate-middle badge rounded-pill bg-info">{cartItem}<span
                            className="visually-hidden">unread messages</span></span>
                </DropdownToggle>
                <DropdownMenu className="dropdown-menu-xl dropdown-menu-end p-0"
                    aria-labelledby="page-header-cart-dropdown">
                    <div className="p-3 border-top-0 border-start-0 border-end-0 border-dashed border">
                        <Row className="align-items-center">
                            <Col>
                                <h6 className="m-0 fs-16 fw-semibold"> My Cart</h6>
                            </Col>
                            <div className="col-auto">
                                <span className="badge badge-soft-warning fs-13"> <span className="cartitem-badge"> {cartItem} </span> items</span>
                            </div>
                        </Row>
                    </div>
                    <SimpleBar style={{ maxHeight: "300px" }}>
                        <div className="p-2">
                            <div className="text-center empty-cart" id="empty-cart" style={{ display: "none" }}>
                                <div className="avatar-md mx-auto my-3">
                                    <div className="avatar-title bg-soft-info text-info fs-36 rounded-circle">
                                        <i className='bx bx-cart'></i>
                                    </div>
                                </div>
                                <h5 className="mb-3">Your Cart is Empty!</h5>
                                <Link to="/apps-ecommerce-products" className="btn btn-success w-md mb-3">Shop Now</Link>
                            </div>
                            {cartData.map((item, key) => (<div className="d-block dropdown-item dropdown-item-cart text-wrap px-3 py-2" key={key}>
                                <div className="d-flex align-items-center">
                                    <img src={item.img}
                                        className="me-3 rounded-circle avatar-sm p-2 bg-light" alt="user-pic" />
                                    <div className="flex-1">
                                        <h6 className="mt-0 mb-1 fs-14">
                                            <Link to="/apps-ecommerce-product-details" className="text-reset">{item.product}</Link>
                                        </h6>
                                        <p className="mb-0 fs-12 text-muted">
                                            Quantity: <span>{item.quantity} x ${item.price}</span>
                                        </p>
                                    </div>
                                    <div className="px-2">
                                        <h5 className="m-0 fw-normal">$<span className="cart-item-price">{item.quantity * item.price}</span></h5>
                                    </div>
                                    <div className="ps-2">
                                        <button type="button" className="btn btn-icon btn-sm btn-ghost-secondary remove-item-btn" onClick={(e) => { removeItem(e.target); }}><i
                                            className="ri-close-fill fs-16"></i></button>
                                    </div>
                                </div>
                            </div>))}
                        </div>
                    </SimpleBar>
                    <div className="p-3 border-bottom-0 border-start-0 border-end-0 border-dashed border" id="checkout-elem">
                        <div className="d-flex justify-content-between align-items-center pb-3">
                            <h5 className="m-0 text-muted">Total:</h5>
                            <div className="px-2">
                                <h5 className="m-0">$<span id="cart-item-total">3400</span></h5>
                            </div>
                        </div>

                        <Link to="/apps-ecommerce-checkout" className="btn btn-success text-center w-100">
                            Checkout
                        </Link>
                    </div>
                </DropdownMenu>
            </Dropdown>
        </React.Fragment>
    );
};

export default MyCartDropdown;