// Import Images
import avatar1 from "../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../assets/images/users/avatar-8.jpg";

import btcCoin from "../../assets/images/svg/crypto-icons/btc.svg";
import ltcCoin from "../../assets/images/svg/crypto-icons/ltc.svg";
import ethCoin from "../../assets/images/svg/crypto-icons/eth.svg";
import dashCoin from "../../assets/images/svg/crypto-icons/dash.svg";

import image3 from "../../assets/images/companies/img-3.png";
import image4 from "../../assets/images/companies/img-4.png";
import image6 from "../../assets/images/companies/img-6.png";
import image8 from "../../assets/images/companies/img-8.png";

const tileBoxs1 = [
    {
        id: 1,
        label: "Total Earnings",
        labelClass: "muted",
        percentage: "+16.24 %",
        percentageClass: "success",
        percentageIcon: "ri-arrow-right-up-line",
        counter: "559.25",
        caption: "View net earnings",
        icon: "bx bx-dollar-circle",
        iconClass: "success",
        decimals: 2,
        prefix: "$",
        suffix: "k",
    },
    {
        id: 2,
        label: "Orders",
        labelClass: "white-50",
        percentage: "-3.57 %",
        percentageClass: "danger",
        percentageIcon: "ri-arrow-right-down-line",
        counter: "36894",
        caption: "View all orders",
        icon: "bx bx-shopping-bag",
        iconClass: "light",
        bgColor: "bg-secondary",
        counterClass: "text-white",
        captionClass: "text-white-50",
        decimals: 0,
        prefix: "",
        separator: ",",
        suffix: "",
    },
    {
        id: 3,
        label: "Customers",
        labelClass: "muted",
        percentage: "+29.08 %",
        percentageClass: "success",
        percentageIcon: "ri-arrow-right-up-line",
        counter: "183.35",
        caption: "See details",
        icon: "bx bx-user-circle",
        iconClass: "warning",
        decimals: 2,
        prefix: "",
        suffix: "M",
    },
    {
        id: 4,
        label: "My Balance",
        labelClass: "muted",
        percentage: "+0.00 %",
        percentageClass: "muted",
        counter: "165.89",
        caption: "Withdraw money",
        icon: "bx bx-wallet",
        iconClass: "primary",
        decimals: 2,
        prefix: "$",
        separator: ",",
        suffix: "k",
    },
];

const tileBoxs2 = [
    {
        id: 1,
        label: "Campaign Sent",
        badge: "ri-arrow-up-circle-line text-success",
        icon: "ri-space-ship-line",
        counter: "197",
        decimals: 0,
        suffix: "",
        prefix: "",
    },
    {
        id: 2,
        label: "Annual Profit",
        badge: "ri-arrow-up-circle-line text-success",
        icon: "ri-exchange-dollar-line",
        counter: "489.4",
        decimals: 1,
        suffix: "k",
        prefix: "$",
    },
    {
        id: 3,
        label: "Lead Coversation",
        badge: "ri-arrow-down-circle-line text-danger",
        icon: "ri-pulse-line",
        counter: "32.89",
        decimals: 2,
        suffix: "%",
        prefix: "",
    },
    {
        id: 4,
        label: "Daily Average Income",
        badge: "ri-arrow-up-circle-line text-success",
        icon: "ri-trophy-line",
        counter: "1596.5",
        decimals: 1,
        prefix: "$",
        separator: ",",
        suffix: "",
    },
    {
        id: 5,
        label: "Annual Deals",
        badge: "ri-arrow-down-circle-line text-danger",
        icon: "ri-service-line",
        counter: "2659",
        decimals: 0,
        separator: ",",
        suffix: "",
        prefix: "",
    },
];

const tileBoxs3 = [
    {
        id: 1,
        label: "Users",
        labelClass: "muted",
        badgeClass: "bg-light text-success",
        badge: "ri-arrow-up-line",
        percentage: "16.24 %",
        iconClass: "info",
        feaIcon: "users",
        subCounter: [
            {
                id: 1,
                counter: "28.05",
                decimals: 2,
                suffix: "k",
                prefix: "",
            },
        ],
    },
    {
        id: 2,
        label: "Sessions",
        labelClass: "muted",
        badgeClass: "bg-light text-danger",
        badge: "ri-arrow-down-line",
        percentage: "3.96 %",
        iconClass: "info",
        feaIcon: "activity",
        subCounter: [
            {
                id: 1,
                counter: "97.66",
                decimals: 2,
                suffix: "k",
                prefix: "",
            },
        ],
    },
    {
        id: 3,
        bgColor: "bg-primary",
        label: "Avg. Visit Duration",
        labelClass: "white-50",
        counterClass: "text-white",
        badgeClass: "badge-soft-light",
        badge: "ri-arrow-down-line",
        percentage: "0.24 %",
        iconClass: "light",
        feaIcon: "clock",
        decimals: 0,
        suffix: "",
        prefix: "",
        subCounter: [
            {
                id: 1,
                counter: "3",
                decimals: 0,
                suffix: "m ",
                prefix: "",
            },
            {
                id: 2,
                counter: "40",
                decimals: 0,
                suffix: "sec",
                prefix: "",
            },
        ],
    },
    {
        id: 4,
        label: "Bounce Rate",
        labelClass: "muted",
        badgeClass: "bg-light text-success",
        badge: "ri-arrow-up-line",
        percentage: "7.05 %",
        iconClass: "info",
        feaIcon: "external-link",
        subCounter: [
            {
                id: 1,
                counter: "33.48",
                decimals: 2,
                suffix: "%",
                prefix: "",
            },
        ],
    },
];

const tileBoxes4 = [
    {
        id: 1,
        bgColor: "bg-danger",
        icon: "bx bx-shopping-bag",
        iconClass: "light",
        label: "Total Sales",
        labelClass: "white-50",
        counter: "2045",
        counterClass: "text-white",
        caption: "From 1930 last year",
        captionClass: "white-50",
        percentage: "6.11 %",
        percentageClass: "light",
        badge: "ri-arrow-up-s-line",
        decimals: 0,
        separator: ",",
        suffix: "",
        prefix: "",
    },
    {
        id: 2,
        icon: "bx bxs-user-account",
        iconClass: "warning",
        label: "Number of Users",
        labelClass: "muted",
        counter: "7522",
        caption: "From 9530 last year",
        captionClass: "muted",
        percentage: "10.35 %",
        percentageClass: "danger",
        badge: "ri-arrow-down-s-line",
        decimals: 0,
        separator: ",",
        suffix: "",
        prefix: "",
    },
    {
        id: 3,
        icon: "bx bxs-badge-dollar",
        iconClass: "danger",
        label: "Total Revenue",
        labelClass: "muted",
        counter: "2845.05",
        caption: "From $1,750.04 last year",
        captionClass: "muted",
        percentage: "22.96 %",
        percentageClass: "success",
        badge: "ri-arrow-up-s-line",
        decimals: 2,
        separator: ",",
        suffix: "",
        prefix: "$",
    },
    {
        id: 4,
        icon: "bx bx-store-alt",
        iconClass: "info",
        label: "Number of Stores",
        labelClass: "muted",
        counter: "405",
        caption: "From 308 last year",
        captionClass: "muted",
        percentage: "16.31 %",
        percentageClass: "success",
        badge: "ri-arrow-up-s-line",
        decimals: 0,
        separator: ",",
        suffix: "k",
        prefix: "",
    },
];

const tileBoxes5 = [
    {
        id: 1,
        img: image3,
        title: "Marketing Director",
        class: "",
        year: "(2-4 Yrs Exp.)"
    },
    {
        id: 2,
        img: image4,
        class: "",
        title: "Sr. Web Designer",
        year: "(2+ Yrs Exp.)"
    },
    {
        id: 3,
        img: image6,
        class: "bg-soft-primary shadow-none bg-opacity-10",
        title: "Marketing Director",
        year: "(2+ Yrs Exp.)"
    },
    {
        id: 4,
        img: image8,
        class: "bg-soft-info shadow-none bg-opacity-10",
        title: "Sr. Web Designer",
        year: "(2+ Yrs Exp.)"
    },
];

const otherWidgets2 = [
    {
        id: 1,
        title: "Brand Logo Design - MD",
        subTitle: "Graphics Work",
        subItem: [
            {
                id: 1,
                icon: "mdi mdi-numeric-1-circle",
                iconClass: "success",
                label: "Completed",
            },
            {
                id: 2,
                icon: "mdi mdi-numeric-3-circle",
                iconClass: "info",
                label: "In Progress",
            },
            {
                id: 3,
                icon: "mdi mdi-numeric-2-circle",
                iconClass: "primary",
                label: "To Do",
            },
        ],
        progressBar: [
            { id: 1, bgColor: "bg-success", width: "30%" },
            { id: 2, bgColor: "bg-info", width: "50%" },
            { id: 3, width: "20%" },
        ],
    },
    {
        id: 2,
        title: "Redesign - Landing Page",
        subTitle: "UI/UX Design",
        subItem: [
            {
                id: 1,
                icon: "mdi mdi-numeric-3-circle",
                iconClass: "success",
                label: "Completed",
            },
            {
                id: 2,
                icon: "mdi mdi-numeric-0-circle",
                iconClass: "info",
                label: "In Progress",
            },
            {
                id: 3,
                icon: "mdi mdi-numeric-8-circle",
                iconClass: "primary",
                label: "To Do",
            },
        ],
        progressBar: [
            { id: 1, bgColor: "bg-success", width: "30%" },
            { id: 2, bgColor: "bg-info", width: "0%" },
            { id: 3, width: "70%" },
        ],
    },
    {
        id: 3,
        title: "Ecommerce App for Web",
        subTitle: "CRM Project",
        subItem: [
            {
                id: 1,
                icon: "mdi mdi-numeric-10-circle",
                iconClass: "success",
                label: "Completed",
            },
            {
                id: 2,
                icon: "mdi mdi-numeric-8-circle",
                iconClass: "info",
                label: "In Progress",
            },
            {
                id: 3,
                icon: "mdi mdi-numeric-2-circle",
                iconClass: "primary",
                label: "To Do",
            },
        ],
        progressBar: [
            { id: 1, bgColor: "bg-success", width: "60%" },
            { id: 2, bgColor: "bg-info", width: "25%" },
            { id: 3, width: "15%" },
        ],
    },
];

const widgetsActivities = [
    {
        id: 1,
        date: "25",
        weekDay: "Tue",
        time: "12:00am - 03:30pm",
        caption: "Meeting for campaign with sales team",
        subItem: [
            { id: 1, img: avatar1 },
            { id: 2, img: avatar2 },
            { id: 3, img: avatar3 },
            { id: 4, imgNumber: "5", bgcolor: "bg-info" },
        ],
    },
    {
        id: 2,
        date: "20",
        weekDay: "Wed",
        time: "02:00pm - 03:45pm",
        caption: "Adding a new event with attachments",
        subItem: [
            { id: 1, img: avatar4 },
            { id: 2, img: avatar5 },
            { id: 3, img: avatar6 },
            { id: 4, img: avatar7 },
            { id: 5, imgNumber: "3", bgcolor: "bg-success" },
        ],
    },
    {
        id: 3,
        date: "17",
        weekDay: "Wed",
        time: "04:30pm - 07:15pm",
        caption: "Create new project Bundling Product",
        subItem: [
            { id: 1, img: avatar8 },
            { id: 2, img: avatar1 },
            { id: 3, img: avatar2 },
            { id: 4, imgNumber: "4", bgcolor: "bg-primary" },
        ],
    },
    {
        id: 4,
        date: "12",
        weekDay: "Tue",
        time: "10:30am - 01:15pm",
        caption: "Weekly closed sales won checking with sales team",
        subItem: [
            { id: 1, img: avatar1 },
            { id: 2, img: avatar5 },
            { id: 3, img: avatar2 },
            { id: 4, imgNumber: "9", bgcolor: "bg-warning" },
        ],
    },
];

const widgetsTasks = [
    {
        id: 1,
        forId: "task_one",
        text: "Review and make sure nothing slips through cracks",
        date: "15 Sep, 2021",
    },
    {
        id: 2,
        forId: "task_two",
        text: "Send meeting invites for sales upcampaign",
        date: "20 Sep, 2021",
    },
    {
        id: 3,
        forId: "task_three",
        text: "Weekly closed sales won checking with sales team",
        date: "24 Sep, 2021",
    },
    {
        id: 4,
        forId: "task_four",
        text: "Add notes that can be viewed from the individual view",
        date: "27 Sep, 2021",
    },
    {
        id: 5,
        forId: "task_five",
        text: "Move stuff to another page",
        date: "27 Sep, 2021",
    },
    {
        id: 6,
        forId: "task_six",
        text: "Styling wireframe design and documentation for velzon admin",
        date: "27 Sep, 2021",
    },
];

const widgetsAudiences = [
    {
        id: 1,
        subCounter: [{ id: 1, counter: "854", suffix: "", prefix: "" }],
        caption: "Avg. Session",
        percentage: "49%",
    },
    {
        id: 2,
        subCounter: [
            {
                id: 1,
                counter: "1278",
                decimals: 0,
                separator: ",",
                suffix: "",
                prefix: "",
            },
        ],
        caption: "Conversion Rate",
        percentage: "60%",

    },
    {
        id: 3,
        subCounter: [
            {
                id: 1,
                counter: "3",
                decimals: 0,
                suffix: "m ",
                prefix: "",
            },
            {
                id: 2,
                counter: "40",
                decimals: 0,
                suffix: "sec",
                prefix: "",
            },
        ],
        caption: "Avg. Ses. Duration",
    },
];

const widgetsPortfolio = [
    {
        id: 1,
        img: btcCoin,
        label: "Bitcoin",
        coinName: "BTC",
        coinVolume: "0.00584875",
        badgeClass: "primary",
        price: "$19,405.12",
        priceClass: "success",
        coinNameClass: "primary"
    },
    {
        id: 2,
        img: ethCoin,
        label: "Ethereum",
        coinName: "ETH",
        coinVolume: "2.25842108",
        badgeClass: "info",
        price: "$40552.18",
        priceClass: "danger",
        coinNameClass: "info"
    },
    {
        id: 3,
        img: ltcCoin,
        label: "Litecoin",
        coinName: "LTC",
        coinVolume: "10.58963217",
        badgeClass: "warning",
        price: "$15824.58",
        priceClass: "success",
        coinNameClass: "warning"
    },
    {
        id: 4,
        img: dashCoin,
        label: "Dash",
        coinName: "DASH",
        coinVolume: "204.28565885",
        badgeClass: "success",
        price: "$30635.84",
        priceClass: "success",
        coinNameClass: "success"
    },
];

export {
    tileBoxs1,
    tileBoxs2,
    tileBoxs3,
    tileBoxes4,
    tileBoxes5,
    widgetsActivities,
    widgetsTasks,
    widgetsAudiences,
    widgetsPortfolio,
    otherWidgets2
};
