import React, { useState } from "react";
import { Link } from "react-router-dom";
import {
  Button,
  Offcanvas,
  OffcanvasBody,
  Dropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownItem,
  UncontrolledDropdown
} from "reactstrap";
import Img9 from "../../assets/images/small/img-9.jpg";
import dummyImage from "../../assets/images/users/user-dummy-img.jpg";
import { attachements } from "../../common/data/chat";

const Attachements = (props) => {
  return (
    <div className="border rounded border-dashed p-2">
      <div className="d-flex align-items-center">
        <div className="flex-shrink-0 me-3">
          <div className="avatar-xs">
            <div className="avatar-title bg-light text-secondary rounded fs-20">
              <i className={props.attachement.foldericon}></i>
            </div>
          </div>
        </div>
        <div className="flex-grow-1 overflow-hidden">
          <h5 className="fs-15 mb-1">
            <Link to="#" className="text-body text-truncate d-block">
              {props.attachement.foldername}
            </Link>
          </h5>
          <div className="text-muted">{props.attachement.foldersize}</div>
        </div>
        <div className="flex-shrink-0 ms-2">
          <div className="d-flex gap-1">
            <button
              type="button"
              className="btn btn-icon text-muted btn-sm fs-18"
            >
              <i className="ri-download-2-line"></i>
            </button>
            <UncontrolledDropdown>
              <DropdownToggle
                tag="button"
                className="btn btn-icon text-muted btn-sm fs-18 dropdown"
              >
                <i className="ri-more-fill"></i>
              </DropdownToggle>
              <DropdownMenu>
                <DropdownItem>
                  <i className="ri-share-line align-bottom me-2 text-muted"></i>{" "}
                  Share
                </DropdownItem>
                <DropdownItem>
                  <i className="ri-bookmark-line align-bottom me-2 text-muted"></i>{" "}
                  Bookmark
                </DropdownItem>
                <DropdownItem>
                  <i className="ri-delete-bin-line align-bottom me-2 text-muted"></i>{" "}
                  Delete
                </DropdownItem>
              </DropdownMenu>
            </UncontrolledDropdown>
          </div>
        </div>
      </div>
    </div>
  );
};

const PersonalInfo = ({ show, onCloseClick, currentuser, cuurentiseImg }) => {
  const [menu1, setMenu1] = useState(false);
  const [menu2, setMenu2] = useState(false);

  return (
    <Offcanvas
      isOpen={show}
      direction="end"
      className="offcanvas-end border-0"
      toggle={onCloseClick}
    >
      <OffcanvasBody className="offcanvas-body profile-offcanvas p-0">
        <div className="team-cover">
          <img src={Img9} alt="" className="img-fluid" />
        </div>
        <div className="p-1 pb-4 pt-0">
          <div className="team-settings">
            <div className="row g-0">
              <div className="col">
                <div className="btn nav-btn">
                  <Button
                    onClick={onCloseClick}
                    color=""
                    className="btn-close btn-close-white"
                  ></Button>
                </div>
              </div>
              <div className="col-auto">
                <div className="user-chat-nav d-flex">
                  <button
                    type="button"
                    className="btn nav-btn favourite-btn active"
                  >
                    <i className="ri-star-fill"></i>
                  </button>

                  <Dropdown isOpen={menu1} toggle={() => setMenu1(!menu1)}>
                    <DropdownToggle className="btn nav-btn" tag="a">
                      <i className="ri-more-2-fill"></i>
                    </DropdownToggle>
                    <DropdownMenu className="dropdown-menu-end">
                      <DropdownItem href="#">
                        <i className="ri-inbox-archive-line align-bottom text-muted me-2"></i>
                        Archive
                      </DropdownItem>
                      <DropdownItem href="#">
                        <i className="ri-mic-off-line align-bottom text-muted me-2"></i>
                        Muted
                      </DropdownItem>
                      <DropdownItem href="#">
                        <i className="ri-delete-bin-5-line align-bottom text-muted me-2"></i>
                        Delete
                      </DropdownItem>
                    </DropdownMenu>
                  </Dropdown>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div className="p-3 text-center">
        {cuurentiseImg ?
        
          <img
            src={cuurentiseImg}
            alt=""
            className="avatar-lg img-thumbnail rounded-circle mx-auto profile-img"
          />
          :
          <img 
          src={dummyImage}
          alt="" 
          className="avatar-lg img-thumbnail rounded-circle mx-auto profile-img" />
        }
          <div className="mt-3">
            <h5 className="fs-16 mb-1">
              <Link to="#" className="link-primary username">
                {currentuser}
              </Link>
            </h5>
            <p className="text-muted">
              <i className="ri-checkbox-blank-circle-fill me-1 align-bottom text-success"></i>
              Online
            </p>
          </div>
          <div className="d-flex gap-3 justify-content-center">
            <button
              type="button"
              className="btn avatar-xs p-0"
              data-bs-toggle="tooltip"
              data-bs-placement="top"
              title="Message"
            >
              <span className="avatar-title rounded bg-light text-body">
                <i className="ri-question-answer-line"></i>
              </span>
            </button>

            <button
              type="button"
              className="btn avatar-xs p-0"
              data-bs-toggle="tooltip"
              data-bs-placement="top"
              title="Favourite"
            >
              <span className="avatar-title rounded bg-light text-body">
                <i className="ri-star-line"></i>
              </span>
            </button>

            <button
              type="button"
              className="btn avatar-xs p-0"
              data-bs-toggle="tooltip"
              data-bs-placement="top"
              title="Phone"
            >
              <span className="avatar-title rounded bg-light text-body">
                <i className="ri-phone-line"></i>
              </span>
            </button>

            <Dropdown isOpen={menu2} toggle={() => setMenu2(!menu2)}>
              <DropdownToggle className="btn avatar-xs p-0" tag="a">
                <span className="avatar-title bg-light text-body rounded">
                  <i className="ri-more-fill"></i>
                </span>
              </DropdownToggle>
              <DropdownMenu className="dropdown-menu-end">
                <DropdownItem href="#">
                  <i className="ri-inbox-archive-line align-bottom text-muted me-2"></i>
                  Archive
                </DropdownItem>
                <DropdownItem href="#">
                  <i className="ri-mic-off-line align-bottom text-muted me-2"></i>
                  Muted
                </DropdownItem>
                <DropdownItem href="#">
                  <i className="ri-delete-bin-5-line align-bottom text-muted me-2"></i>
                  Delete
                </DropdownItem>
              </DropdownMenu>
            </Dropdown>
          </div>
        </div>
        <div className="border-top border-top-dashed p-3">
          <h5 className="fs-15 mb-3">Personal Details</h5>
          <div className="mb-3">
            <p className="text-muted text-uppercase fw-medium fs-13 mb-1">
              Number
            </p>
            <h6>+(256) 2451 8974</h6>
          </div>
          <div className="mb-3">
            <p className="text-muted text-uppercase fw-medium fs-13 mb-1">
              Email
            </p>
            <h6>lisaparker@gmail.com</h6>
          </div>
          <div>
            <p className="text-muted text-uppercase fw-medium fs-13 mb-1">
              Location
            </p>
            <h6 className="mb-0">California, USA</h6>
          </div>
        </div>

        <div className="border-top border-top-dashed p-3">
          <h5 className="fs-15 mb-3">Attached Files</h5>

          <div className="vstack gap-2">
            {attachements.map((attachement, key) => (
              <Attachements attachement={attachement} key={key} />
            ))}

            <div className="text-center mt-2">
              <button type="button" className="btn btn-danger">
                Load more{" "}
                <i className="ri-arrow-right-fill align-bottom ms-1"></i>
              </button>
            </div>
          </div>
        </div>
      </OffcanvasBody>
    </Offcanvas>
  );
};

export default PersonalInfo;
