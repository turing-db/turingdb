import React from "react";

const EcommerceOrderProduct = (props) => {
  return (
    <React.Fragment>
      <tr>
        <td>
          <div className="d-flex">
            <div className="flex-shrink-0 avatar-md bg-light rounded p-1">
              <img
                src={props.product.img}
                alt=""
                className="img-fluid d-block"
              />
            </div>
            <div className="flex-grow-1 ms-3">
              <h5 className="fs-16">
                <a
                  href="apps-ecommerce-product-details"
                  className="link-primary"
                >
                  {props.product.name}
                </a>
              </h5>
              <p className="text-muted mb-0">
                Color: <span className="fw-medium">{props.product.color}</span>
              </p>
              <p className="text-muted mb-0">
                Size: <span className="fw-medium">{props.product.size}</span>
              </p>
            </div>
          </div>
        </td>
        <td>{props.product.price}</td>
        <td>{props.product.quantity}</td>
        <td>
          <div className="text-warning fs-15">
            <i className="ri-star-fill"></i>
            <i className="ri-star-fill"></i>
            <i className="ri-star-fill"></i>
            <i className="ri-star-fill"></i>
            <i className="ri-star-half-fill"></i>
          </div>
        </td>
        <td className="fw-medium text-end">{props.product.amount}</td>
      </tr>
    </React.Fragment>
  );
};

export default EcommerceOrderProduct;
