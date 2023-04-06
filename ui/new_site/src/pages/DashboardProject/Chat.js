import React, { useState, useEffect, useCallback } from 'react';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';

import { dashboardChat } from '../../common/data';

//Import Scrollbar
import PerfectScrollbar from "react-perfect-scrollbar";
import "react-perfect-scrollbar/dist/css/styles.css";

const Chat = () => {
    const [text, setText] = useState("");
    const [messages, setMessages] = useState([...dashboardChat]);
    const [messageBox, setMessageBox] = useState(null);

    const scrollToBottom = useCallback(() => {
        if (messageBox) {
            messageBox.scrollTop = messageBox.scrollHeight + 1000;
        }
    }, [messageBox]);

    useEffect(() => {
        if ((messages || []).length > 1) {
            scrollToBottom();
        }
    }, [messages, scrollToBottom]);


    const onSendMessage = () => {
        var modifiedMessages = [...messages];
        const lastItem = modifiedMessages.length
            ? modifiedMessages[modifiedMessages.length - 1]
            : { id: 1 };
        const today = new Date();
        const hour = today.getHours();
        const minute = today.getMinutes();
        const senderObj = {
            id: lastItem["id"] + 1,
            message: text,
            time: `${hour}.${minute}`,
            isLeft: false,
        };
        modifiedMessages.push({ ...senderObj });
        setMessages(modifiedMessages);
        setText("");
    };
    return (
        <React.Fragment>
            <Col xxl={4} lg={6}>
                <Card className="card-height-100">
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Chat</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown" direction='start'>
                                <DropdownToggle tag="a" className="text-reset" role="button">
                                    <span className="text-muted"><i className="ri-settings-4-line align-middle me-1"></i>Setting <i className="mdi mdi-chevron-down ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu-end">
                                    <DropdownItem><i className="ri-user-2-fill align-bottom text-muted me-2"></i> View Profile</DropdownItem>
                                    <DropdownItem><i className="ri-inbox-archive-line align-bottom text-muted me-2"></i> Archive</DropdownItem>
                                    <DropdownItem><i className="ri-mic-off-line align-bottom text-muted me-2"></i> Muted</DropdownItem>
                                    <DropdownItem><i className="ri-delete-bin-5-line align-bottom text-muted me-2"></i> Delete</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>

                    <CardBody className="p-0">
                    <div id="users-chat">
                            <PerfectScrollbar className="chat-conversation p-3" id="chat-conversation"
                                style={{ marginBottom: "1rem", maxHeight: "400px" }}
                                containerRef={ref => setMessageBox(ref)}
                            >
                                <ul className="list-unstyled chat-conversation-list chat-sm" id="users-conversation">
                                    {(messages || []).map((item, key) => (
                                        <li className={item.isLeft ? "chat-list left" : "chat-list right"} key={key}>
                                            <div className="conversation-list">
                                                {item.img ? <div className="chat-avatar">
                                                    <img src={item.img} alt="" />
                                                </div> : null}
                                                <div className="user-chat-content">
                                                    <div className="ctext-wrap">
                                                        <React.Fragment>
                                                            <div className="ctext-wrap-content">
                                                                <p className="mb-0 ctext-content">{item.message}</p>
                                                            </div>
                                                            <UncontrolledDropdown className="align-self-start message-box-drop">
                                                                <DropdownToggle tag="a" role="button">
                                                                    <i className="ri-more-2-fill"></i>
                                                                </DropdownToggle>
                                                                <DropdownMenu>
                                                                    <DropdownItem><i className="ri-reply-line me-2 text-muted align-bottom"></i>Reply</DropdownItem>
                                                                    <DropdownItem><i className="ri-share-line me-2 text-muted align-bottom"></i>Forward</DropdownItem>
                                                                    <DropdownItem><i className="ri-file-copy-line me-2 text-muted align-bottom"></i>Copy</DropdownItem>
                                                                    <DropdownItem><i className="ri-bookmark-line me-2 text-muted align-bottom"></i>Bookmark</DropdownItem>
                                                                    <DropdownItem className="delete-item"><i className="ri-delete-bin-5-line me-2 text-muted align-bottom"></i>Delete</DropdownItem>
                                                                </DropdownMenu>
                                                            </UncontrolledDropdown>
                                                        </React.Fragment>
                                                    </div>
                                                    <div className="conversation-name"><small className="text-muted time">{item.time}</small> <span className="text-success check-message-icon"><i className="ri-check-double-line align-bottom"></i></span></div>
                                                </div>
                                            </div>
                                        </li>))}
                                </ul>
                            </PerfectScrollbar>
                        </div>
                        <div className="border-top border-top-dashed">
                            <div className="row g-2 mx-3 mt-2 mb-3">
                                <div className="col">
                                    <div className="position-relative">
                                        <input type="text" className="form-control border-light bg-light" placeholder="Enter Message..." 
                                        value={text} onChange={e => setText(e.target.value)}/>
                                    </div>
                                </div>
                                <div className="col-auto">
                                    <button type="submit" className="btn btn-info" onClick={() => onSendMessage()}><span className="d-none d-sm-inline-block me-2">Send</span> <i className="mdi mdi-send float-end"></i></button>
                                </div>
                            </div>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default Chat;