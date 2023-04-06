import React, { useEffect, useState } from 'react';
import PropTypes from "prop-types";
import { Link } from 'react-router-dom';
import withRouter from '../../Components/Common/withRouter';
import { Col, Collapse, Row } from 'reactstrap';
// Import Data
import navdata from "../LayoutMenuData";
//i18n
import { withTranslation } from "react-i18next";

const HorizontalLayout = (props) => {
    const [isMoreMenu, setIsMoreMenu] = useState(false);
    const navData = navdata().props.children;
    let menuItems = [];
    let splitMenuItems = [];
    let menuSplitContainer = 6;
    navData.forEach(function (value, key) {
        if (value['isHeader']) {
            menuSplitContainer++;
        }
        if (key >= menuSplitContainer) {
            let val = value;
            val.childItems = value.subItems;
            val.isChildItem = (value.subItems) ? true : false;
            delete val.subItems;
            splitMenuItems.push(val);
        } else {
            menuItems.push(value);
        }
    });
    menuItems.push({ id: 'more', label: 'More', icon: 'ri-briefcase-2-line', link: "/#", stateVariables: isMoreMenu, subItems: splitMenuItems, click: function (e) { e.preventDefault(); setIsMoreMenu(!isMoreMenu); }, });

    useEffect(() => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
        const initMenu = () => {
            const pathName = process.env.PUBLIC_URL + props.router.location.pathname;
            const ul = document.getElementById("navbar-nav");
            const items = ul.getElementsByTagName("a");
            let itemsArray = [...items]; // converts NodeList to Array
            removeActivation(itemsArray);
            let matchingMenuItem = itemsArray.find((x) => {
                return x.pathname === pathName;
            });
            if (matchingMenuItem) {
                activateParentDropdown(matchingMenuItem);
            }
        };
        initMenu();
    }, [props.router.location.pathname, props.layoutType]);

    function activateParentDropdown(item) {
        item.classList.add("active");
        let parentCollapseDiv = item.closest(".collapse.menu-dropdown");

        if (parentCollapseDiv) {

            // to set aria expand true remaining
            parentCollapseDiv.classList.add("show");
            parentCollapseDiv.parentElement.children[0].classList.add("active");
            parentCollapseDiv.parentElement.children[0].setAttribute("aria-expanded", "true");
            if (parentCollapseDiv.parentElement.closest(".collapse.menu-dropdown")) {
                parentCollapseDiv.parentElement.closest(".collapse").classList.add("show");
                var parentElementDiv = parentCollapseDiv.parentElement.closest(".collapse").previousElementSibling;
                if (parentElementDiv)
                    if(parentElementDiv.closest(".collapse"))
                        parentElementDiv.closest(".collapse").classList.add("show")
                    parentElementDiv.classList.add("active");
                var parentElementSibling = parentElementDiv.parentElement.parentElement.parentElement.previousElementSibling;
                if (parentElementSibling) {
                    parentElementSibling.classList.add("active");
                }
            }
            return false;
        }
        return false;
    }

    const removeActivation = (items) => {
        let actiItems = items.filter((x) => x.classList.contains("active"));

        actiItems.forEach((item) => {
            if (item.classList.contains("menu-link")) {
                if (!item.classList.contains("active")) {
                    item.setAttribute("aria-expanded", false);
                }
                if (item.nextElementSibling) {
                    item.nextElementSibling.classList.remove("show");
                }
            }
            if (item.classList.contains("nav-link")) {
                if (item.nextElementSibling) {
                    item.nextElementSibling.classList.remove("show");
                }
                item.setAttribute("aria-expanded", false);
            }
            item.classList.remove("active");
        });
    };

    return (
        <React.Fragment>
            {(menuItems || []).map((item, key) => {
                return (
                    <React.Fragment key={key}>
                        {/* Main Header */}
                        {!item['isHeader'] ?
                            (item.subItems ? (
                                <li className="nav-item">
                                    <Link
                                        onClick={item.click}
                                        className="nav-link menu-link"
                                        to={item.link ? item.link : "/#"}
                                        data-bs-toggle="collapse"
                                    >
                                        <i className={item.icon}></i> <span data-key="t-apps">{props.t(item.label)}</span>
                                    </Link>
                                    <Collapse
                                        className={item.id === "baseUi" && item.subItems.length > 13 ? "menu-dropdown mega-dropdown-menu" : "menu-dropdown"}
                                        isOpen={item.stateVariables}
                                        id="sidebarApps">
                                        {/* subItms  */}
                                        {item.id === "baseUi" && item.subItems.length > 13 ? (
                                            <React.Fragment>
                                                <Row>
                                                    {item.subItems && ((item.subItems || []).map((subItem, key) => (
                                                        <React.Fragment key={key}>
                                                            {key % 2 === 0 ? (
                                                                <Col lg={4}>
                                                                    <ul className="nav nav-sm flex-column">
                                                                        <li className="nav-item">
                                                                            <Link to={item.subItems[key].link} className="nav-link">{item.subItems[key].label}</Link>
                                                                        </li>
                                                                    </ul>
                                                                </Col>
                                                            ) : (
                                                                <Col lg={4}>
                                                                    <ul className="nav nav-sm flex-column">
                                                                        <li className="nav-item">
                                                                            <Link to={item.subItems[key].link} className="nav-link">{item.subItems[key].label}</Link>
                                                                        </li>
                                                                    </ul>
                                                                </Col>
                                                            )
                                                            }
                                                        </React.Fragment>
                                                    ))
                                                    )}
                                                </Row>
                                            </React.Fragment>
                                        ) : (
                                            <ul className="nav nav-sm flex-column test">
                                                {item.subItems && ((item.subItems || []).map((subItem, key) => (
                                                    <React.Fragment key={key}>
                                                        {!subItem.isChildItem ? (
                                                            <li className="nav-item">
                                                                <Link
                                                                    to={subItem.link ? subItem.link : "/#"}
                                                                    className="nav-link"
                                                                >
                                                                    {props.t(subItem.label)}
                                                                </Link>
                                                            </li>
                                                        ) : (
                                                            <li className="nav-item">
                                                                <Link
                                                                    onClick={subItem.click}
                                                                    className="nav-link"
                                                                    to="/#"
                                                                    data-bs-toggle="collapse"
                                                                > {props.t(subItem.label)}
                                                                </Link>
                                                                <Collapse className="menu-dropdown" isOpen={subItem.stateVariables} id="sidebarEcommerce">
                                                                    <ul className="nav nav-sm flex-column">
                                                                        {/* child subItms  */}
                                                                        {subItem.childItems && (
                                                                            (subItem.childItems || []).map((subChildItem, key) => (
                                                                                <React.Fragment key={key}>
                                                                                    {!subChildItem.isChildItem ? (
                                                                                        <li className="nav-item">
                                                                                            <Link
                                                                                                to={subChildItem.link ? subChildItem.link : "/#"}
                                                                                                className="nav-link"
                                                                                            >
                                                                                                {props.t(subChildItem.label)}
                                                                                            </Link>
                                                                                        </li>
                                                                                    ) : (
                                                                                        <li className="nav-item">
                                                                                            <Link
                                                                                                onClick={subChildItem.click}
                                                                                                className="nav-link"
                                                                                                to="/#"
                                                                                                data-bs-toggle="collapse"
                                                                                            > {props.t(subChildItem.label)}
                                                                                            </Link>
                                                                                            <Collapse className="menu-dropdown" isOpen={subChildItem.stateVariables} id="sidebarEcommerce">
                                                                                                <ul className="nav nav-sm flex-column">
                                                                                                    {/* child subItms  */}
                                                                                                    {subChildItem.childItems && (
                                                                                                        (subChildItem.childItems || []).map((subSubChildItem, key) => (
                                                                                                            <li className="nav-item apex" key={key}>
                                                                                                                <Link
                                                                                                                    to={subSubChildItem.link ? subSubChildItem.link : "/#"}
                                                                                                                    className="nav-link">
                                                                                                                    {props.t(subSubChildItem.label)}
                                                                                                                </Link>
                                                                                                            </li>
                                                                                                        ))
                                                                                                    )}
                                                                                                </ul>
                                                                                            </Collapse>
                                                                                        </li>
                                                                                    )}
                                                                                </React.Fragment>
                                                                            ))
                                                                        )}
                                                                    </ul>
                                                                </Collapse>
                                                            </li>
                                                        )}
                                                    </React.Fragment>
                                                ))
                                                )}
                                            </ul>
                                        )}
                                    </Collapse>
                                </li>
                            ) : (
                                <li className="nav-item">
                                    <Link
                                        className="nav-link menu-link"
                                        to={item.link ? item.link : "/#"}>
                                        <i className={item.icon}></i> <span>{props.t(item.label)}</span>
                                    </Link>
                                </li>
                            ))
                            : (<li className="menu-title"><span data-key="t-menu">{props.t(item.label)}</span></li>)}
                    </React.Fragment>
                );
            })}
            {/* menu Items */}
        </React.Fragment>
    );
};

HorizontalLayout.propTypes = {
    location: PropTypes.object,
    t: PropTypes.any,
};

export default withRouter(withTranslation()(HorizontalLayout));
