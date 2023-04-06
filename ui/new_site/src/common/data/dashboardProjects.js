// Import Images
import avatar1 from "../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../assets/images/users/avatar-8.jpg";

import img1 from "../../assets/images/small/img-1.jpg";
import img2 from "../../assets/images/small/img-2.jpg";

const projectsWidgets = [
    {
        id: 1,
        feaIcon: "briefcase",
        feaIconClass: "primary",
        label: "Active Projects",
        badgeClass: "danger",
        icon: "ri-arrow-down-s-line",
        percentage: "5.02 %",
        caption: "Projects this month",
        subCounter: [{ id: 1, counter: "825", suffix: "" }]
    },
    {
        id: 2,
        feaIcon: "award",
        feaIconClass: "primary",
        label: "New Leads",
        badgeClass: "success",
        icon: "ri-arrow-up-s-line",
        percentage: "3.58 %",
        caption: "Leads this month",
        subCounter: [{ id: 1, counter: "7522", suffix: "", separator: "," }]
    },
    {
        id: 3,
        feaIcon: "clock",
        feaIconClass: "primary",
        label: "Total Hours",
        badgeClass: "danger",
        icon: "ri-arrow-down-s-line",
        percentage: "10.35 %",
        caption: "Work this month",
        subCounter: [{ id: 1, counter: "168", suffix: "h" }, { id: 1, counter: "40", suffix: "m" }]
    }
];

const overviewCounter = [
    {
        id: 1,
        counter: "9851",
        label: "Number of Projects",
        decimals: 0,
        prefix: "",
        separator: ",",
        suffix: "",
    },
    {
        id: 2,
        counter: "1026",
        label: "Active Projects",
        decimals: 0,
        prefix: "",
        separator: ",",
        suffix: "",
    },
    {
        id: 3,
        counter: "228.89",
        label: "Revenue",
        decimals: 2,
        prefix: "$",
        separator: ",",
        suffix: "K",
    },
    {
        id: 4,
        counter: "10589",
        label: "Working Hours",
        decimals: 0,
        prefix: "",
        separator: ",",
        suffix: "h",
        counterClass: "success"
    },
];

const activeProjects = [
    {
        id: 1,
        projectName: "Brand Logo Design",
        img: avatar1,
        projectLead: "Donald Risher",
        percentage: "53%",
        subItem: [
            { id: 1, assImg: avatar1 },
            { id: 2, assImg: avatar2 },
            { id: 3, assImg: avatar3 },
        ],
        badge: "Inprogress",
        badgeClass: "warning",
        dueDate: "06 Sep 2021",
    },
    {
        id: 2,
        projectName: "Redesign - Landing Page",
        img: avatar2,
        projectLead: "Prezy William",
        percentage: "0%",
        subItem: [
            { id: 1, assImg: avatar5 },
            { id: 2, assImg: avatar6 },
        ],
        badge: "Pending",
        badgeClass: "danger",
        dueDate: "13 Nov 2021",
    },
    {
        id: 3,
        projectName: "Multipurpose Landing Template",
        img: avatar3,
        projectLead: "Boonie Hoynas",
        percentage: "100%",
        subItem: [
            { id: 1, assImg: avatar7 },
            { id: 2, assImg: avatar8 },
        ],
        badge: "Completed",
        badgeClass: "success",
        dueDate: "26 Nov 2021",
    },
    {
        id: 4,
        projectName: "Chat Application",
        img: avatar5,
        projectLead: "Pauline Moll",
        percentage: "64%",
        subItem: [{ id: 1, assImg: avatar2 }],
        badge: "Progress",
        badgeClass: "warning",
        dueDate: "15 Dec 2021",
    },
    {
        id: 5,
        projectName: "Create Wireframe",
        img: avatar6,
        projectLead: "James Bangs",
        percentage: "77%",
        subItem: [
            { id: 1, assImg: avatar1 },
            { id: 2, assImg: avatar6 },
            { id: 3, assImg: avatar4 },
        ],
        badge: "Progress",
        badgeClass: "warning",
        dueDate: "21 Dec 2021",
    },
];

const projectTasks = [
    {
        id: 1,
        forId: "checkTask1",
        label: "Create new Admin Template",
        dedline: "03 Nov 2021",
        status: "Completed",
        statusClass: "success",
        img: avatar2,
    },
    {
        id: 2,
        forId: "checkTask2",
        label: "Marketing Coordinator",
        dedline: "17 Nov 2021",
        status: "Progress",
        statusClass: "warning",
        img: avatar7,
    },
    {
        id: 3,
        forId: "checkTask3",
        label: "Administrative Analyst",
        dedline: "26 Nov 2021",
        status: "Completed",
        statusClass: "success",
        img: avatar6,
    },
    {
        id: 4,
        forId: "checkTask4",
        label: "E-commerce Landing Page",
        dedline: "10 Dec 2021",
        status: "Pending",
        statusClass: "danger",
        img: avatar5,
    },
    {
        id: 5,
        forId: "checkTask5",
        label: "UI/UX Design",
        dedline: "22 Dec 2021",
        status: "Progress",
        statusClass: "warning",
        img: avatar1,
    },
    {
        id: 6,
        forId: "checkTask6",
        label: "Projects Design",
        dedline: "31 Dec 2021",
        status: "Pending",
        statusClass: "danger",
        img: avatar4,
    },
];

const teamMembers = [
    {
        id: 1,
        img: avatar1,
        name: "Donald Risher",
        position: "Product Manager",
        hours: "110",
        tasks: "258",
        series: "50",
        chartsColor: "#25a0e2"
    },
    {
        id: 2,
        img: avatar2,
        name: "Jansh Brown",
        position: "Lead Developer",
        hours: "83",
        tasks: "105",
        series: "45",
        chartsColor: "#25a0e2"
    },
    {
        id: 3,
        img: avatar7,
        name: "Carroll Adams",
        position: "Lead Designer",
        hours: "58",
        tasks: "75",
        series: "75",
        chartsColor: "#25a0e2"
    },
    {
        id: 4,
        img: avatar4,
        name: "William Pinto",
        position: "UI/UX Designer",
        hours: "96",
        tasks: "85",
        series: "25",
        chartsColor: "#25a0e2"
    },
    {
        id: 5,
        img: avatar6,
        name: "Garry Fournier",
        position: "Web Designer",
        hours: "76",
        tasks: "69",
        series: "60",
        chartsColor: "#25a0e2"
    },
    {
        id: 6,
        img: avatar5,
        name: "Susan Denton",
        position: "Lead Designer",
        hours: "123",
        tasks: "658",
        series: "85",
        chartsColor: "#25a0e2"
    },
    {
        id: 7,
        img: avatar3,
        name: "Joseph Jackson",
        position: "React Developer",
        hours: "117",
        tasks: "125",
        series: "70",
        chartsColor: "#25a0e2"
    },
];

const dashboardChat = [
    {
        id: 1,
        isLeft: true,
        img: avatar2,
        message: "Good morning 😊",
        time: "09:07 am",
    },
    {
        id: 2,
        isLeft: false,
        message: "Good morning, How are you? What about our next meeting?",
        time: "09:08 am",
    },
    {
        id: 3,
        isLeft: true,
        img: avatar2,
        message: "Yeah everything is fine. Our next meeting tomorrow at 10.00 AM",
        time: "09:10 am",
    },
    {
        id: 4,
        isLeft: false,
        message: "Wow that's great",
        time: "09:12 am",
    },
    {
        id: 5,
        isLeft: true,
        img: avatar2,
        message: "Good morning 😊",
        time: "09:30 am",
    },
];


const allProjectData = [{
    name: 'Number of Projects',
    type: 'bar',
    data: [34, 65, 46, 68, 49, 61, 42, 44, 78, 52, 63, 67]
}, {
    name: 'Revenue',
    type: 'area',
    data: [89.25, 98.58, 68.74, 108.87, 77.54, 84.03, 51.24, 28.57, 92.57, 42.36, 88.51, 36.57]
}, {
    name: 'Active Projects',
    type: 'bar',
    data: [8, 12, 7, 17, 21, 11, 5, 9, 7, 29, 12, 35]
}];

const monthProjectData = [{
    name: 'Number of Projects',
    type: 'bar',
    data: [14, 45, 56, 78, 79, 81, 62, 44, 88, 52, 63, 87]
}, {
    name: 'Revenue',
    type: 'area',
    data: [119.25, 128.58, 148.74, 148.87, 17.54, 154.03, 71.24, 78.57, 92.57, 42.36, 88.51, 76.57]
}, {
    name: 'Active Projects',
    type: 'bar',
    data: [18, 22, 17, 47, 71, 31, 5, 9, 7, 29, 22, 75]
}];

const halfyearProjectData = [{
    name: 'Number of Projects',
    type: 'bar',
    data: [34, 75, 66, 78, 29, 41, 32, 44, 58, 22, 43, 77]
}, {
    name: 'Revenue',
    type: 'area',
    data: [109.25, 48.58, 38.74, 78.87, 57.54, 44.03, 21.24, 18.57, 92.57, 42.36, 48.51, 56.57]
}, {
    name: 'Active Projects',
    type: 'bar',
    data: [38, 22, 27, 47, 61, 41, 15, 89, 27, 49, 32, 75]
}];

const yearProjectData = [{
    name: 'Number of Projects',
    type: 'bar',
    data: [14, 45, 16, 38, 29, 21, 12, 14, 58, 22, 23, 47]
}, {
    name: 'Revenue',
    type: 'area',
    data: [59.25, 68.58, 48.74, 78.87, 57.54, 64.03, 21.24, 18.57, 52.57, 22.36, 68.51, 16.57]
}, {
    name: 'Active Projects',
    type: 'bar',
    data: [38, 32, 27, 37, 41, 51, 35, 29, 17, 59, 32, 45]
}];

// Project Status
const allTimeData = [225, 152, 178, 189];

const lastWeekData = [145, 62, 68, 79];

const lastMonthData = [155, 82, 78, 99];

const lastquarterData = [185, 142, 158, 149];


export { projectsWidgets, overviewCounter, activeProjects, projectTasks, teamMembers, allProjectData, monthProjectData, halfyearProjectData, yearProjectData, allTimeData, lastWeekData, lastMonthData, lastquarterData, dashboardChat };