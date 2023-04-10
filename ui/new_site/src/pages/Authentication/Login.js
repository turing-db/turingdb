import React, { useEffect, useState } from 'react';
import { Card, CardBody, Col, Container, Input, Label, Row, Button, Form, FormFeedback, Alert, Spinner } from 'reactstrap';

//redux
import { useSelector, useDispatch } from "react-redux";

import { Link } from "react-router-dom";
import withRouter from "../../Components/Common/withRouter";
// Formik validation
import * as Yup from "yup";
import { useFormik } from "formik";

//Social Media Imports
import { GoogleLogin } from "react-google-login";
// import TwitterLogin from "react-twitter-auth"
import FacebookLogin from "react-facebook-login/dist/facebook-login-render-props";
// actions
import { loginUser, socialLogin, resetLoginFlag } from "../../slices/thunks";

import logoLight from "../../assets/images/logo-light.png";
//Import config
import { facebook, google } from "../../config";
//import images

const Login = (props) => {
    const dispatch = useDispatch();
    const { user } = useSelector(state => ({
        user: state.Account.user,
    }));

    const [userLogin, setUserLogin] = useState([]);
    const [passwordShow, setPasswordShow] = useState(false);

    useEffect(() => {
        if (user && user) {
            setUserLogin({
                email: user.user.email,
                password: user.user.confirm_password
            });
        }
    }, [user]);

    const validation = useFormik({
        // enableReinitialize : use this flag when initial values needs to be changed
        enableReinitialize: true,

        initialValues: {
            email: userLogin.email || "admin@themesbrand.com" || '',
            password: userLogin.password || "123456" || '',
        },
        validationSchema: Yup.object({
            email: Yup.string().required("Please Enter Your Email"),
            password: Yup.string().required("Please Enter Your Password"),
        }),
        onSubmit: (values) => {
            console.log("onSubmit");
            dispatch(loginUser(values, props.router.navigate));
        }
    });

    const { error, errorFlag, loading } = useSelector(state => ({
        error: state.Login.error,
        loading: state.Login.loading,
        errorFlag: state.Login.errorFlag,
    }));

    const signIn = (res, type) => {
        if (type === "google" && res) {
            const postData = {
                name: res.profileObj.name,
                email: res.profileObj.email,
                token: res.tokenObj.access_token,
                idToken: res.tokenId,
            };
            dispatch(socialLogin(postData, props.router.navigate, type));
        } else if (type === "facebook" && res) {
            const postData = {
                name: res.name,
                email: res.email,
                token: res.accessToken,
                idToken: res.tokenId,
            };
            dispatch(socialLogin(postData, props.router.navigate, type));
        }
    };

    //handleGoogleLoginResponse
    const googleResponse = response => {
        signIn(response, "google");
    };

    //handleTwitterLoginResponse
    // const twitterResponse = e => {}

    //handleFacebookLoginResponse
    const facebookResponse = response => {
        signIn(response, "facebook");
    };

    useEffect(() => {
        if (errorFlag) {
            setTimeout(() => {
                dispatch(resetLoginFlag());
            }, 3000);
        }
    }, [dispatch, errorFlag]);

    document.title = "Turing Platform";
    return (
        <React.Fragment>
            <div className="auth-page-wrapper auth-bg-cover py-5 d-flex justify-content-center align-items-center min-vh-100">
                <div className="bg-overlay"></div>
                <div className="auth-page-content overflow-hidden pt-lg-5">
                    <Container>

                        <Row className="justify-content-center">
                            <Col md={8} lg={6} xl={5}>
                                <Card className="mt-4">
                                    <CardBody className="p-4">
                                        <div className="text-center mt-2">
                                            <h5 className="text-primary">Welcome Back !</h5>
                                        </div>
                                        {error && error ? (<Alert color="danger"> {error} </Alert>) : null}
                                        <div className="p-2 mt-4">
                                            <Form
                                                onSubmit={(e) => {
                                                    e.preventDefault();
                                                    validation.handleSubmit();
                                                    return false;
                                                }}
                                                action="#">

                                                <div className="mb-3">
                                                    <Label htmlFor="email" className="form-label">Email</Label>
                                                    <Input
                                                        name="email"
                                                        className="form-control"
                                                        placeholder="Enter email"
                                                        type="email"
                                                        onChange={validation.handleChange}
                                                        onBlur={validation.handleBlur}
                                                        value={validation.values.email || ""}
                                                        invalid={
                                                            validation.touched.email && validation.errors.email ? true : false
                                                        }
                                                    />
                                                    {validation.touched.email && validation.errors.email ? (
                                                        <FormFeedback type="invalid">{validation.errors.email}</FormFeedback>
                                                    ) : null}
                                                </div>

                                                <div className="mb-3">
                                                    <div className="float-end">
                                                        <Link to="/forgot-password" className="text-muted">Forgot password?</Link>
                                                    </div>
                                                    <Label className="form-label" htmlFor="password-input">Password</Label>
                                                    <div className="position-relative auth-pass-inputgroup mb-3">
                                                        <Input
                                                            name="password"
                                                            value={validation.values.password || ""}
                                                            type={passwordShow ? "text" : "password"}
                                                            className="form-control pe-5"
                                                            placeholder="Enter Password"
                                                            onChange={validation.handleChange}
                                                            onBlur={validation.handleBlur}
                                                            invalid={
                                                                validation.touched.password && validation.errors.password ? true : false
                                                            }
                                                        />
                                                        {validation.touched.password && validation.errors.password ? (
                                                            <FormFeedback type="invalid">{validation.errors.password}</FormFeedback>
                                                        ) : null}
                                                        <button className="btn btn-link position-absolute end-0 top-0 text-decoration-none text-muted" type="button" id="password-addon" onClick={() => setPasswordShow(!passwordShow)}><i className="ri-eye-fill align-middle"></i></button>
                                                    </div>
                                                </div>

                                                <div className="form-check">
                                                    <Input className="form-check-input" type="checkbox" value="" id="auth-remember-check" />
                                                    <Label className="form-check-label" htmlFor="auth-remember-check">Remember me</Label>
                                                </div>

                                                <div className="mt-4">
                                                    <Button color="success" disabled={error ? null : loading ? true : false} className="btn btn-success w-100" type="submit">
                                                        {loading ? <Spinner size="sm" className='me-2'> Loading... </Spinner> : null}
                                                        Sign In
                                                    </Button>
                                                </div>

                                                <div className="mt-4 text-center">
                                                    <div className="signin-other-title">
                                                        <h5 className="fs-13 mb-4 title">Sign In with</h5>
                                                    </div>
                                                    <div>
                                                        <FacebookLogin
                                                            appId={facebook.APP_ID}
                                                            autoLoad={false}
                                                            callback={facebookResponse}
                                                            render={renderProps => (
                                                                <Button color="primary"
                                                                    className="btn-icon me-1"
                                                                    onClick={renderProps.onClick}
                                                                >
                                                                    <i className="ri-facebook-fill fs-16" />
                                                                </Button>
                                                            )}
                                                        />
                                                        <GoogleLogin
                                                            clientId={
                                                                google.CLIENT_ID ? google.CLIENT_ID : ""
                                                            }
                                                            render={renderProps => (
                                                                <Button color="danger"
                                                                    to="#"
                                                                    className="btn-icon me-1"
                                                                    onClick={renderProps.onClick}
                                                                >
                                                                    <i className="ri-google-fill fs-16" />
                                                                </Button>
                                                            )}
                                                            onSuccess={googleResponse}
                                                            onFailure={() => {

                                                            }}
                                                        />
                                                        <Button color="dark" className="btn-icon"><i className="ri-github-fill fs-16"></i></Button>{" "}
                                                        <Button color="info" className="btn-icon"><i className="ri-twitter-fill fs-16"></i></Button>
                                                    </div>
                                                </div>
                                            </Form>
                                        </div>
                                    </CardBody>
                                </Card>

                            </Col>
                        </Row>
                    </Container>
                </div>
                <footer className="footer start-0">
                    <Container>
                        <Row>
                            <Col lg={12}>
                                <div className="text-center">
                                    <p className="mb-0">&copy; {new Date().getFullYear()} Turing Biosystems</p>
                                </div>
                            </Col>
                        </Row>
                    </Container>
                </footer>
            </div>
        </React.Fragment>
    );
};

export default withRouter(Login);
