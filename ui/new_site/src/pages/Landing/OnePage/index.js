import React from 'react';

import Navbar from './navbar';
import Home from './home';
import Client from './client';
import Services from './services';
import Features from './features';
import Plans from './plans';
import Faqs from './faq';
import Reviews from './reviews';
import Counter from './counter';
import WorkProcess from './workProcess';
import Team from './team';
import Contact from './contact';
import Cta from './cta';
import Footer from './footer';

const Index = () => {
document.title =" Landing | Velzon - React Admin & Dashboard Template";

window.onscroll = function () {
    scrollFunction();
};

const scrollFunction = () => {
    const element = document.getElementById("back-to-top");
    if (element) {
        if (document.body.scrollTop > 100 || document.documentElement.scrollTop > 100) {
            element.style.display = "block";
        } else {
            element.style.display = "none";
        }
    }
};

const toTop = () => {
    document.body.scrollTop = 0;
    document.documentElement.scrollTop = 0;
};

    return (
        <React.Fragment>            
            <div className="layout-wrapper landing">
                <Navbar />
                <Home />
                <Client />
                <Services />
                <Features />
                <Plans />
                <Faqs />
                <Reviews />
                <Counter />
                <WorkProcess />
                <Team />
                <Contact />
                <Cta />
                <Footer />
                <button onClick={() => toTop()} className="btn btn-danger btn-icon landing-back-top" id="back-to-top">
                    <i className="ri-arrow-up-line"></i>
                </button>
            </div>
        </React.Fragment>
    );
};

export default Index;