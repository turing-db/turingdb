import img1 from "../../assets/images/companies/img-1.png";
import img3 from "../../assets/images/companies/img-3.png";
import img2 from "../../assets/images/companies/img-2.png";
import img4 from "../../assets/images/companies/img-4.png";
import img5 from "../../assets/images/companies/img-5.png";

import avtar1 from "../../assets/images/users/avatar-1.jpg";
import avtar2 from "../../assets/images/users/avatar-2.jpg";
import avtar3 from "../../assets/images/users/avatar-3.jpg";
import avtar4 from "../../assets/images/users/avatar-4.jpg";
import avtar6 from "../../assets/images/users/avatar-6.jpg";
import avtar10 from "../../assets/images/users/avatar-10.jpg";
import avtar8 from "../../assets/images/users/avatar-8.jpg";
import avtar9 from "../../assets/images/users/avatar-9.jpg";

const jobWidgets = [
    {
        id: 1,
        lable: "TOTAL JOBS",
        jobs: "36,894",
        series: [95],
        color: "#09b39b",
    },
    {
        id: 2,
        lable: "APPLY JOBS",
        jobs: "28,410",
        series: [97],
        color: "#09b39b",
    },
    {
        id: 3,
        lable: "NEW JOBS",
        jobs: "4,305",
        series: [80],
        color: "#09b39b",
    },
    {
        id: 4,
        lable: "INTERVIEW",
        jobs: "5,021",
        series: [89],
        color: "#f06548",
    },
    {
        id: 5,
        lable: "HIRED",
        jobs: "3,948",
        series: [64],
        color: "#09b39b",
    },
    {
        id: 6,
        lable: "REJECTED",
        jobs: "1,340",
        series: [20],
        color: "#f06548",
    },
];

const featuredCompany = [
    {
        id: 1,
        img: img1,
        lable: "Force Medicines",
        city: " Cullera, Spain",
    },
    {
        id: 2,
        img: img3,
        lable: "Syntyce Solutions",
        city: "Mughairah, UAE",
    },
    {
        id: 3,
        img: img2,
        lable: "Moetic Fashion",
        city: "Mughairah, UAE",
    },
    {
        id: 4,
        img: img4,
        lable: "Meta4Systems",
        city: "Germany",
    },
    {
        id: 5,
        img: img5,
        lable: "Themesbrand",
        city: " Limestone, US",
    },
];

const recentApplicants = [
    {
        id: 1,
        no: "#VZ2112",
        img: avtar1,
        name: "Nicholas Ball",
        designation: "Assistant / Store Keeper",
        rate: "$109.00",
        city: "California, US",
        type: "Full Time",
        typeColor: "success",
        rating: "5.0",
        ratingDetails: "(245 Rating)"
    },
    {
        id: 2,
        no: "#VZ2111",
        img: avtar2,
        name: "Elizabeth Allen",
        designation: "Education Training",
        rate: "$149.00",
        city: "Mughairah, UAE",
        type: "Freelancer",
        typeColor: "secondary",
        rating: "4.7",
        ratingDetails: "(1.34 Rating)"
    },
    {
        id: 3,
        no: "#VZ2114",
        img: avtar3,
        name: "Cassian Jenning",
        designation: "Graphic Designer",
        rate: "$215.00",
        city: "4,305",
        type: "Part Time",
        typeColor: "danger",
        rating: "4.5",
        ratingDetails: "(2.93 Rating)"
    },
    {
        id: 4,
        no: "#VZ2117",
        img: avtar4,
        name: "Scott Holt",
        designation: "UI/UX Designer",
        rate: "$235.00",
        city: "Germany",
        type: "Part Time",
        typeColor: "danger",
        rating: "2.3",
        ratingDetails: "(4.5 Rating)"
    },
    {
        id: 5,
        no: "#VZ2113",
        img: avtar6,
        name: "Hadley Leonard",
        designation: "React Developer",
        rate: "$175.00",
        city: " Limestone, US",
        type: "Full Time",
        typeColor: "success",
        rating: "4.5",
        ratingDetails: "(645 Rating)"
    },
    {
        id: 6,
        no: "#VZ2118",
        img: avtar10,
        name: "Harley Watkins",
        designation: "Project Manager",
        rate: "$372.00",
        city: " Limestone, US",
        type: "Freelancer",
        typeColor: "secondary",
        rating: "4.5",
        ratingDetails: "(12 Rating)"
    },
    {
        id: 7,
        no: "#VZ2119",
        img: avtar9,
        name: "Jenson Carlson",
        designation: "Web Designer",
        rate: "$835.00",
        city: " Limestone, US",
        type: "Part Time",
        typeColor: "danger",
        rating: "2.5",
        ratingDetails: "(6.3 Rating)"
    },
    {
        id: 8,
        no: "#VZ2115",
        img: avtar8,
        name: "Nadia Harding",
        designation: "Product Director",
        rate: "$215.00",
        city: " Limestone, US",
        type: "Full Time",
        typeColor: "success",
        rating: "4.7",
        ratingDetails: "(23 Rating)"
    },
];
export { jobWidgets, featuredCompany, recentApplicants };
