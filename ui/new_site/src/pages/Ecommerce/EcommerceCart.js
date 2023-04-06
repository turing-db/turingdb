import React, { useState, useEffect } from "react";
import { shoppingCart } from "../../common/data/ecommerce";

//Import Breadcrumb
import BreadCrumb from "../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

import {
  Card,
  CardBody,
  Col,
  Container,
  Input,
  Row,
  CardHeader,
  UncontrolledAlert,
} from "reactstrap";

const EcommerceCart = () => {

  document.title="Shopping Cart | Velzon - React Admin & Dashboard Template";

  const [productList, setproductList] = useState(shoppingCart);
  const [charge, setCharge] = useState(0);
  const [tax, setTax] = useState(0);
  const [dis, setDis] = useState(0);

  const assigned = productList.map((item) => item.total);
  let subTotal = 0;
  for (let i = 0; i < assigned.length; i++) {
    subTotal += Math.round(assigned[i]);
  }

  useEffect(() => {
    let dis = (0.15 * subTotal);
    let tax = (0.125 * subTotal);

    if (subTotal !== 0) {
      setCharge(65);
    } else {
      setCharge(0);
    }
    setTax(dis);
    setDis(tax);
  }, [subTotal]);

  function removeCartItem(id) {
    var filtered = productList.filter(function (item) {
      return item.id !== id;
    });

    setproductList(filtered);
  }

  function countUP(id, prev_data_attr, itemPrice) {
    setproductList(
      productList.map((p) =>
        p.id === id ? { ...p, data_attr: prev_data_attr + 1, total: (prev_data_attr + 1) * itemPrice } : p
      )
    );
  }

  function countDown(id, prev_data_attr, itemPrice) {
    setproductList(
      productList.map((p) =>
        (p.id === id && p.data_attr > 0) ? { ...p, data_attr: prev_data_attr - 1, total: (prev_data_attr - 1) * itemPrice } : p
      )
    );
  }

  return (
    <React.Fragment>
      <div className="page-content">       
        <Container fluid>
          <BreadCrumb title="Shopping Cart" pageTitle="Ecommerce" />

          <Row className="mb-3">
            <Col xl={8}>
              <Row className="align-items-center gy-3 mb-3">
                <div className="col-sm">
                  <div>
                    <h5 className="fs-15 mb-0">Your Cart ({productList.length} items)</h5>
                  </div>
                </div>
                <div className="col-sm-auto">
                  <Link
                    to="/apps-ecommerce-products"
                    className="link-primary text-decoration-underline"
                  >
                    Continue Shopping
                  </Link>
                </div>
              </Row>
              {productList.map((cartItem, key) => (
                <React.Fragment key={cartItem.id}>
                  <Card className="product">
                    <CardBody>
                      <Row className="gy-3">
                        <div className="col-sm-auto">
                          <div className="avatar-lg bg-light rounded p-1">
                            <img
                              src={cartItem.img}
                              alt=""
                              className="img-fluid d-block"
                            />
                          </div>
                        </div>
                        <div className="col-sm">
                          <h5 className="fs-15 text-truncate">
                            <Link
                              to="/ecommerce-product-detail"
                              className="text-dark"
                            >
                              {cartItem.name}
                            </Link>
                          </h5>
                          <ul className="list-inline text-muted">
                            <li className="list-inline-item">
                              Color :{" "}
                              <span className="fw-medium">
                                {cartItem.color}
                              </span>
                            </li>
                            <li className="list-inline-item">
                              Size :{" "}
                              <span className="fw-medium">{cartItem.size}</span>
                            </li>
                          </ul>

                          <div className="input-step">
                            <button
                              type="button"
                              className="minus"
                              onClick={() => {
                                countDown(cartItem.id, cartItem.data_attr, cartItem.price);
                              }}
                            >
                              –
                            </button>
                            <Input
                              type="text"
                              className="product-quantity"
                              value={cartItem.data_attr}
                              name="demo_vertical"
                              readOnly
                            />
                            <button
                              type="button"
                              className="plus"
                              onClick={() => {
                                countUP(cartItem.id, cartItem.data_attr, cartItem.price);
                              }}
                            >
                              +
                            </button>
                          </div>
                        </div>
                        <div className="col-sm-auto">
                          <div className="text-lg-end">
                            <p className="text-muted mb-1">Item Price:</p>
                            <h5 className="fs-15">
                              $
                              <span id="ticket_price" className="product-price">
                                {cartItem.price}
                              </span>
                            </h5>
                          </div>
                        </div>
                      </Row>
                    </CardBody>

                    <div className="card-footer">
                      <div className="row align-items-center gy-3">
                        <div className="col-sm">
                          <div className="d-flex flex-wrap my-n1">
                            <div>
                              <Link
                                to="#"
                                className="d-block text-body p-1 px-2"
                                onClick={() => removeCartItem(cartItem.id)}
                              >
                                <i className="ri-delete-bin-fill text-muted align-bottom me-1"></i>{" "}
                                Remove
                              </Link>
                            </div>
                            <div>
                              <Link
                                to="#"
                                className="d-block text-body p-1 px-2"
                              >
                                <i className="ri-star-fill text-muted align-bottom me-1"></i>{" "}
                                Add Wishlist
                              </Link>
                            </div>
                          </div>
                        </div>
                        <div className="col-sm-auto">
                          <div className="d-flex align-items-center gap-2 text-muted">
                            <div>Total :</div>
                            <h5 className="fs-15 mb-0">
                              $
                              <span className="product-line-price">
                                {" "}
                                {(cartItem.total).toFixed(2)}
                              </span>
                            </h5>
                          </div>
                        </div>
                      </div>
                    </div>
                  </Card>
                </React.Fragment>
              ))}

              <div className="text-end mb-4">
                <Link
                  to="/apps-ecommerce-checkout"
                  className="btn btn-success btn-label right ms-auto"
                >
                  <i className="ri-arrow-right-line label-icon align-bottom fs-16 ms-2"></i>{" "}
                  Checkout
                </Link>
              </div>
            </Col>

            <Col xl={4}>
              <div className="sticky-side-div">
                <Card>
                  <CardHeader className="border-bottom-dashed">
                    <h5 className="card-title mb-0">Order Summary</h5>
                  </CardHeader>
                  <CardHeader className="bg-soft-light border-bottom-dashed">
                    <div className="text-center">
                      <h6 className="mb-2 fs-15">
                        Have a <span className="fw-semibold">promo</span> code ?
                      </h6>
                    </div>
                    <div className="hstack gap-3 px-3 mx-n3">
                      <input
                        className="form-control me-auto"
                        type="text"
                        placeholder="Enter coupon code"
                        aria-label="Add Promo Code here..."
                      />
                      <button type="button" className="btn btn-success w-xs">
                        Apply
                      </button>
                    </div>
                  </CardHeader>
                  <CardBody className="pt-2">
                    <div className="table-responsive">
                      <table className="table table-borderless mb-0">
                        <tbody>
                          <tr>
                            <td>Sub Total :</td>
                            <td className="text-end" id="cart-subtotal">
                              $ {subTotal}
                            </td>
                          </tr>
                          <tr>
                            <td>
                              Discount{" "}
                              <span className="text-muted">(VELZON15)</span> :{" "}
                            </td>
                            <td className="text-end" id="cart-discount">
                              - $ {dis}
                            </td>
                          </tr>
                          <tr>
                            <td>Shipping Charge :</td>
                            <td className="text-end" id="cart-shipping">
                              $ {charge}
                            </td>
                          </tr>
                          <tr>
                            <td>Estimated Tax (12.5%) : </td>
                            <td className="text-end" id="cart-tax">
                              $ {tax}
                            </td>
                          </tr>
                          <tr className="table-active">
                            <th>Total (USD) :</th>
                            <td className="text-end">
                              <span className="fw-semibold" id="cart-total">
                                ${subTotal + charge + tax - dis}
                              </span>
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </CardBody>
                </Card>

                <UncontrolledAlert color="danger" className="border-dashed">
                  <div className="d-flex align-items-center">
                    <lord-icon
                      src="https://cdn.lordicon.com/nkmsrxys.json"
                      trigger="loop"
                      colors="primary:#121331,secondary:#fa896b"
                      style={{ width: "80px", height: "80px" }}
                    ></lord-icon>
                    <div className="ms-2">
                      <h5 className="fs-15 text-danger fw-semibold">
                        {" "}
                        Buying for a loved one?
                      </h5>
                      <p className="text-black mb-1">
                        Gift wrap and personalised message on card, <br />
                        Only for <span className="fw-semibold">$9.99</span> USD
                      </p>
                      <button
                        type="button"
                        className="btn ps-0 btn-sm btn-link text-danger text-uppercase"
                      >
                        Add Gift Wrap
                      </button>
                    </div>
                  </div>
                </UncontrolledAlert>
              </div>
            </Col>
          </Row>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default EcommerceCart;
