// Import Images
import smallImage1 from '../../assets/images/small/img-1.jpg';
import smallImage2 from '../../assets/images/small/img-2.jpg';
import smallImage3 from '../../assets/images/small/img-3.jpg';
import smallImage4 from '../../assets/images/small/img-4.jpg';
import smallImage5 from '../../assets/images/small/img-5.jpg';
import smallImage6 from '../../assets/images/small/img-6.jpg';
import smallImage7 from '../../assets/images/small/img-7.jpg';
import smallImage8 from '../../assets/images/small/img-8.jpg';
import smallImage9 from '../../assets/images/small/img-9.jpg';
import smallImage10 from '../../assets/images/small/img-10.jpg';
import smallImage11 from '../../assets/images/small/img-11.jpg';
import smallImage12 from '../../assets/images/small/img-12.jpg';

//User Images
import avatar1 from '../../assets/images/users/avatar-1.jpg';
import avatar2 from '../../assets/images/users/avatar-2.jpg';
import avatar3 from '../../assets/images/users/avatar-3.jpg';
import avatar5 from '../../assets/images/users/avatar-5.jpg';
import avatar4 from '../../assets/images/users/avatar-4.jpg';
import avatar6 from '../../assets/images/users/avatar-6.jpg';
import avatar7 from '../../assets/images/users/avatar-7.jpg';
import avatar8 from '../../assets/images/users/avatar-8.jpg';

const gallery = [
    {
        id: 1,
        img: smallImage1,
        title: "Glasses and laptop from above",
        auther: "Ron Mackie",
        likes: "2.2K",
        comments: "1.3K",
        category: "Project"
    },
    {
        id: 2,
        img: smallImage2,
        title: "Working at a coffee shop",
        auther: "Nancy Martino",
        likes: "2.2K",
        comments: "1.3K",
        category: "Project"
    },
    {
        id: 3,
        img: smallImage3,
        title: "Photo was taken in Beach",
        auther: "Elwood Arter",
        likes: "2.2K",
        comments: "1.3K",
        category: "Project"
    },
    {
        id: 4,
        img: smallImage4,
        title: "Drawing a sketch",
        auther: "Jason McQuaid",
        likes: "2.2K",
        comments: "1.3K",
        category: "Project"
    },
    {
        id: 5,
        img: smallImage5,
        title: "Working from home little spot",
        auther: "Henry Baird",
        likes: "2.2K",
        comments: "1.3K",
        category: "Project"
    },
    {
        id: 6,
        img: smallImage6,
        title: "Glasses and laptop from above",
        auther: "Erica Kernan",
        likes: "2.2K",
        comments: "1.3K",
        category: "Designing"
    },
    {
        id: 7,
        img: smallImage7,
        title: "Sunrise above a beach",
        auther: "James Ballard",
        likes: "2.2K",
        comments: "1.3K",
        category: "Development"
    },
    {
        id: 8,
        img: smallImage8,
        title: "Project discussion with team",
        auther: "Ruby Griffin",
        likes: "2.2K",
        comments: "1.3K",
        category: "Designing"
    },
    {
        id: 9,
        img: smallImage9,
        title: "Dramatic clouds at the Golden Gate Bridge",
        auther: "Ron Mackie",
        likes: "2.2K",
        comments: "1.3K",
        category: "Photography"
    },
    {
        id: 10,
        img: smallImage10,
        title: "Fun day at the Hill Station",
        auther: "Henry Baird",
        likes: "2.2K",
        comments: "1.3K",
        category: "Development"
    },
    {
        id: 11,
        img: smallImage11,
        title: "Cycling in the countryside",
        auther: "Nancy Martino",
        likes: "2.2K",
        comments: "1.3K",
        category: "Designing"
    },
    {
        id: 12,
        img: smallImage12,
        title: "A mix of friends and strangers heading off to find an adventure.",
        auther: "Erica Kernan",
        likes: "2.2K",
        comments: "1.3K",
        category: "Designing"
    },
    {
        id: 13,
        img: smallImage8,
        title: "Project discussion with team",
        auther: "Ruby Griffin",
        likes: "2.2K",
        comments: "1.3K",
        category: "Photography"
    },
];

const pricing1 = [
    {
        id: 1,
        type: "Starter",
        rate: 19,
        description: "The perfect way to get started and get used to our tools.",
        projects: 3,
        Customers: 299,
        FTP: 5,
        supportClass: "danger",
        supportClassSymbol: "close",
        storageClass: "danger",
        storageClassSymbol: "close",
        domainClass: "danger",
        domainClassSymbol: "close",
        ribbon: false,
        planButtonClassname: "success",
        btntxt: "Your Current Plan",
        btnstatus: " disabled ",
        rateYear: "$171",
        delrateYear: "$228"
    },
    {
        id: 2,
        type: "Professional",
        rate: 29,
        description: "Excellent for scalling teams to build culture. Special plan for professional business.",
        projects: 8,
        Customers: 499,
        FTP: 7,
        supportClass: "success",
        supportClassSymbol: "checkbox",
        storageClass: "danger",
        storageClassSymbol: "close",
        domainClass: "danger",
        domainClassSymbol: "close",
        ribbon: false,
        planButtonClassname: "primary",
        btntxt: "Change Plan",
        rateYear: "261",
        delrateYear: "348"
    },
    {
        id: 3,
        type: "Enterprise",
        rate: 39,
        description: "This plan is for those who have a team alredy and running a large business.",
        projects: 15,
        Customers: "Unlimited",
        FTP: 12,
        supportClass: "success",
        supportClassSymbol: "checkbox",
        storageClass: "success",
        storageClassSymbol: "checkbox",
        domainClass: "danger",
        domainClassSymbol: "close",
        ribbon: true,
        planButtonClassname: "primary",
        btntxt: "Change Plan",
        rateYear: "351",
        delrateYear: "468"
    },
    {
        id: 4,
        type: "Unlimited",
        rate: 49,
        description: "For most businesses that want to optimize web queries.",
        projects: "Unlimited",
        Customers: "Unlimited",
        FTP: "Unlimited ",
        supportClass: "success",
        supportClassSymbol: "checkbox",
        storageClass: "success",
        storageClassSymbol: "checkbox",
        domainClass: "success",
        domainClassSymbol: "checkbox",
        ribbon: false,
        planButtonClassname: "primary",
        btntxt: "Change Plan",
        rateYear: "441",
        delrateYear: "588"
    },
];

const pricing2 = [
    {
        id: 1,
        type: "Basic Plan",
        purpose: "For Startup",
        planIcon: "ri-book-mark-line",
        rate: 19,
        projects: 3,
        Customers: 299,
        FTP: 5,
        supportClass: "danger",
        supportClassSymbol: "close",
        storageClass: "danger",
        storageClassSymbol: "close",
        domainClass: "danger",
        domainClassSymbol: "close",
        ribbon: false,
        planButtonClassname: "soft-primary",
        icon: "ri-book-mark-line",
        btntxt: "Sign up free"
    },
    {
        id: 2,
        type: "Pro Business",
        purpose: "Professional plans",
        planIcon: "ri-medal-line",
        rate: 29,
        projects: 15,
        Customers: "unlimited",
        FTP: 12,
        supportClass: "success",
        supportClassSymbol: "checkbox",
        storageClass: "danger",
        storageClassSymbol: "close",
        domainClass: "danger",
        domainClassSymbol: "close",
        ribbon: true,
        planButtonClassname: "primary",
        icon: "ri-medal-line",
        btntxt: "Get started"
    },
    {
        id: 3,
        type: "Platinum Plan",
        purpose: "Enterprise Businesses",
        planIcon: "ri-stack-line",
        rate: 39,
        projects: "unlimited",
        Customers: "unlimited",
        FTP: "unlimited",
        supportClass: "success",
        supportClassSymbol: "checkbox",
        storageClass: "success",
        storageClassSymbol: "checkbox",
        domainClass: "success",
        domainClassSymbol: "checkbox",
        ribbon: false,
        planButtonClassname: "soft-primary",
        icon: "ri-stack-line",
        btntxt: "Get started"
    },
];

const pricing3 = [
    {
        id: 1,
        type: "Starter",
        purpose: "Starter plans",
        rate: 22,
        users: 1,
        storage: "01",
        domain: "No",
        support: "No",
        ribbon: false
    },
    {
        id: 2,
        type: "Professional",
        purpose: "Professional plans",
        rate: 29,
        users: 3,
        storage: "10",
        domain: "Yes",
        support: "No",
        ribbon: true
    },
    {
        id: 3,
        type: "Enterprise",
        purpose: "Enterprise plans",
        rate: 39,
        users: 3,
        storage: "20",
        domain: "Yes",
        support: "Yes",
        ribbon: true
    },
    {
        id: 4,
        type: "Unlimited",
        purpose: "Unlimited plans",
        rate: 49,
        users: 5,
        storage: "40",
        domain: "Yes",
        support: "Yes",
        ribbon: false
    },
];

const projects = [
    {
        id: 1,
        title: "Chat App Update",
        updatedTime: "2 year Ago",
        badgeText: "Inprogress",
        badgeClass: "warning",
        member: [
            {
                id: 1,
                img: avatar1
            },
            {
                id: 2,
                img: avatar3
            }
        ],
        cardBorderColor: "warning",
        memberName: [
            {
                id: 1,
                memberText: "J"
            }
        ]
    },
    {
        id: 2,
        title: "ABC Project Customization",
        updatedTime: "2 month Ago",
        badgeText: "Progress",
        badgeClass: "primary",
        member: [
            {
                id: 1,
                img: avatar8
            },
            {
                id: 2,
                img: avatar7
            },
            {
                id: 3,
                img: avatar6
            },
        ],
        cardBorderColor: "success",
        memberName: [
            {
                id: 1,
                memberText: "2+"
            }
        ]
    },
    {
        id: 3,
        title: "Client - Frank Hook",
        updatedTime: "1 hr Ago",
        badgeText: "New",
        badgeClass: "info",
        member: [
            {
                id: 1,
                img: avatar4
            },
            {
                id: 2,
                img: avatar3
            },

        ],
        cardBorderColor: "info",
        memberName: [
            {
                id: 1,
                memberText: "M"
            }
        ]
    },
    {
        id: 4,
        title: "Velzon Project",
        updatedTime: "11 hr Ago",
        badgeText: "Completed",
        badgeClass: "success",
        member: [
            {
                id: 1,
                img: avatar7
            },
            {
                id: 2,
                img: avatar5
            },

        ],
        cardBorderColor: "primary",
    },
    {
        id: 5,
        title: "Brand Logo Design",
        updatedTime: "10 min Ago",
        badgeText: "New",
        badgeClass: "info",
        member: [
            {
                id: 1,
                img: avatar7
            },
            {
                id: 2,
                img: avatar6
            },

        ],
        cardBorderColor: "danger",
        memberName: [
            {
                id: 1,
                memberText: "E"
            }
        ]
    },
    {
        id: 6,
        title: "Chat App",
        updatedTime: "8 hr Ago",
        badgeText: "Inprogress",
        badgeClass: "warning",
        member: [
            {
                id: 1,
                img: avatar3
            },
            {
                id: 2,
                img: avatar8
            },
        ],
        memberName: [
            {
                id: 1,
                memberText: "R"
            }
        ],
        cardBorderColor: "primary"
    },
    {
        id: 7,
        title: "Project Update",
        updatedTime: "48 min Ago",
        badgeText: "Inprogress",
        badgeClass: "warning",
        member: [
            {
                id: 1,
                img: avatar6
            },
            {
                id: 2,
                img: avatar5
            },
            {
                id: 3,
                img: avatar4
            },
        ],
        cardBorderColor: "warning"
    },
    {
        id: 8,
        title: "Client - Jennifer",
        updatedTime: "30 min Ago",
        badgeText: "Process",
        badgeClass: "primary",
        member: [
            {
                id: 1,
                img: avatar1
            }
        ],
        cardBorderColor: "success"
    },
    {
        id: 9,
        title: "Business Template -UI/UX design",
        updatedTime: "7 month Ago",
        badgeText: "Completed",
        badgeClass: "success",
        member: [
            {
                id: 1,
                img: avatar2
            },
            {
                id: 2,
                img: avatar3
            },
            {
                id: 3,
                img: avatar4
            }
        ],
        cardBorderColor: "info",
        memberName: [
            {
                id: 1,
                memberText: "2+"
            }
        ]
    },
    {
        id: 10,
        title: "Update Project",
        updatedTime: "1 month Ago",
        badgeText: "New",
        badgeClass: "info",
        member: [
            {
                id: 1,
                img: avatar7
            }
        ],
        memberName: [
            {
                id: 1,
                memberText: "A"
            }
        ],
        cardBorderColor: "success"
    },
    {
        id: 11,
        title: "Bank Management System",
        updatedTime: "10 month Ago",
        badgeText: "Completed",
        badgeClass: "success",
        member: [
            {
                id: 1,
                img: avatar7
            },
            {
                id: 2,
                img: avatar6
            },
            {
                id: 3,
                img: avatar5
            }
        ],
        cardBorderColor: "danger",
        memberName: [
            {
                id: 1,
                memberText: "2+"
            }
        ]
    },
    {
        id: 12,
        title: "PSD to HTML Convert",
        updatedTime: "29 min Ago",
        badgeText: "New",
        badgeClass: "info",
        member: [
            {
                id: 1,
                img: avatar7
            }
        ],
        cardBorderColor: "primary"
    },

];

const document = [
    {
        id: 1,
        icon: "ri-file-zip-fill",
        iconBackgroundClass: "primary",
        fileName: "Artboard-documents.zip",
        fileType: "Zip File",
        fileSize: "4.57 MB",
        updatedDate: "12 Dec 2021"
    },
    {
        id: 2,
        icon: "ri-file-pdf-fill",
        iconBackgroundClass: "danger",
        fileName: "Bank Management System",
        fileType: "PDF File",
        fileSize: "8.89 MB",
        updatedDate: "24 Nov 2021"
    },
    {
        id: 3,
        icon: "ri-video-line",
        iconBackgroundClass: "secondary",
        fileName: "Tour-video.mp4",
        fileType: "MP4 File",
        fileSize: "14.62 MB",
        updatedDate: "19 Nov 2021"
    },
    {
        id: 4,
        icon: "ri-file-excel-fill",
        iconBackgroundClass: "secondary",
        fileName: "Tour-video.mp4",
        fileType: "XSL File",
        fileSize: "2.38 KB",
        updatedDate: "14 Nov 2021"
    },
    {
        id: 5,
        icon: "ri-folder-line",
        iconBackgroundClass: "info",
        fileName: "Project Screenshots Collection",
        fileType: "Folder File",
        fileSize: "87.24 MB",
        updatedDate: "08 Nov 2021"
    },
    {
        id: 6,
        icon: "ri-image-2-fill",
        iconBackgroundClass: "danger",
        fileName: "Velzon-logo.png",
        fileType: "PNG File",
        fileSize: "879 KB",
        updatedDate: "02 Nov 2021"
    },
];

const SearchGallery = [
    {
        id: 1,
        img: smallImage1,
        title: "Glasses and laptop from above",
        auther: "Ron Mackie",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 2,
        img: smallImage2,
        title: "Working at a coffee shop",
        auther: "Nancy Martino",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 3,
        img: smallImage3,
        title: "Photo was taken in Beach",
        auther: "Elwood Arter",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 4,
        img: smallImage4,
        title: "Drawing a sketch",
        auther: "Jason McQuaid",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 5,
        img: smallImage5,
        title: "Working from home little spot",
        auther: "Henry Baird",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 6,
        img: smallImage6,
        title: "Glasses and laptop from above",
        auther: "Erica Kernan",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 7,
        img: smallImage7,
        title: "Sunrise above a beach",
        auther: "James Ballard",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 8,
        img: smallImage8,
        title: "Project discussion with team",
        auther: "Ruby Griffin",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 9,
        img: smallImage9,
        title: "Dramatic clouds at the Golden Gate Bridge",
        auther: "Ron Mackie",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 10,
        img: smallImage10,
        title: "Fun day at the Hill Station",
        auther: "Henry Baird",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 11,
        img: smallImage11,
        title: "Cycling in the countryside",
        auther: "Nancy Martino",
        likes: "2.2K",
        comments: "1.3K"
    },
    {
        id: 12,
        img: smallImage12,
        title: "A mix of friends and strangers heading off to find an adventure.",
        auther: "Erica Kernan",
        likes: "2.2K",
        comments: "1.3K"
    },

];

const news = [
    {
        id: 1,
        img: smallImage1,
        badgeClass: "success",
        badgeText: "Business",
        title: "A mix of friends and strangers heading off to find an adventure",
        auther: "James Ballard",
        date: "23 Nov, 2021"
    },
    {
        id: 2,
        img: smallImage2,
        badgeClass: "warning",
        badgeText: "Development",
        title: "How to get creative in your work ?",
        auther: "Ruby Griffin",
        date: "23 Nov, 2021"
    },
    {
        id: 3,
        img: smallImage3,
        badgeClass: "info",
        badgeText: "Fashion",
        title: "How to become a best sale marketer in a year!",
        auther: "Elwood Arter",
        date: "23 Nov, 2021"
    },
    {
        id: 4,
        img: smallImage4,
        badgeClass: "primary",
        badgeText: "Product",
        title: "Manage white space in responsive layouts ?",
        auther: "Nancy Martino",
        date: "23 Nov, 2021"
    },
    {
        id: 5,
        img: smallImage5,
        badgeClass: "success",
        badgeText: "Business",
        title: "Stack designer Olivia Murphy offers freelancing advice",
        auther: "Erica Kernan",
        date: "11 Nov, 2021"
    },
    {
        id: 6,
        img: smallImage6,
        badgeClass: "danger",
        badgeText: "Design",
        title: "A day in the of a professional fashion designer",
        auther: "Jason McQuaid",
        date: "14 Nov, 2021"
    },
    {
        id: 7,
        img: smallImage7,
        badgeClass: "danger",
        badgeText: "Design",
        title: "Design your apps in your own way",
        auther: "Henry Baird",
        date: "19 Nov, 2021"
    },
    {
        id: 8,
        img: smallImage8,
        badgeClass: "warning",
        badgeText: "Development",
        title: "How apps is changing the IT world",
        auther: "Elwood Arter",
        date: "10 Aug, 2021"
    },
];

const video = [
    {
        id: 1,
        title: "Admin dashboard templates - Material Design for Velzon",
        siteLink: "https://themesbrand.com/velzon/index.html",
        videoLink: "https://www.youtube.com/embed/GfSZtaoc5bw",
        description: "Velzon admin is super flexible, powerful, clean, modern & responsive admin template based on bootstrap 5 stable with unlimited possibilities. You can simply change to any layout or mode by changing a couple of lines of code. You can start small and large projects or update design in your existing project using Velzon it is very quick and easy as it is beautiful, adroit, and delivers the ultimate user experience.",
        likes: 335,
        comments: 102,
        auther: "Themesbrand"
    },
    {
        id: 2,
        title: "Create Responsive Admin Dashboard using Html CSS",
        siteLink: "https://themesbrand.com/velzon/index.html",
        videoLink: "https://www.youtube.com/embed/Z-fV2lGKnnU",
        description: "Velzon admin is super flexible, powerful, clean, modern & responsive admin template based on bootstrap 5 stable with unlimited possibilities. You can simply change to any layout or mode by changing a couple of lines of code. You can start small and large projects or update design in your existing project using Velzon it is very quick and easy as it is beautiful, adroit, and delivers the ultimate user experience.",
        likes: 485,
        comments: 167,
        auther: "Themesbrand"
    },
    {
        id: 3,
        title: "Velzon - The Most Popular Bootstrap 5 HTML, Angular & React Js Admin",
        siteLink: "https://themesbrand.com/velzon/index.html",
        videoLink: "https://www.youtube.com/embed/1y_kfWUCFDQ",
        description: "Velzon admin is super flexible, powerful, clean, modern & responsive admin template based on bootstrap 5 stable with unlimited possibilities. You can simply change to any layout or mode by changing a couple of lines of code. You can start small and large projects or update design in your existing project using Velzon it is very quick and easy as it is beautiful, adroit, and delivers the ultimate user experience.",
        likes: 122,
        comments: 51,
        auther: "Themesbrand"
    }
];

const swiper = [
    {
        id: 1,
        img: smallImage12,
        title: "Bootstrap"
    },
    {
        id: 2,
        img: smallImage11,
        title: "Website"
    },
    {
        id: 3,
        img: smallImage10,
        title: "Responsive"
    },
    {
        id: 4,
        img: smallImage9,
        title: "Design"
    },
    {
        id: 5,
        img: smallImage8,
        title: "eCommerce"
    },
    {
        id: 6,
        img: smallImage7,
        title: "Templates"
    },
    {
        id: 7,
        img: smallImage6,
        title: "Admin Panel"
    },
    {
        id: 8,
        img: smallImage5,
        title: "Simple"
    },
    {
        id: 9,
        img: smallImage3,
        title: "Dark"
    },
];

const team = [
    {
        id: 1,
        backgroundImg: smallImage9,
        userImage: avatar2,
        name: "Nancy Martino",
        designation: "Team Leader & HR",
        projectCount: 225,
        taskCount: 197
    },
    {
        id: 2,
        backgroundImg: smallImage12,
        userImage: null,
        userShortName: "HB",
        name: "Henry Baird",
        designation: "Full Stack Developer",
        projectCount: 352,
        taskCount: 376,
        bgColor: "danger",
        textColor: "danger",
    },
    {
        id: 3,
        backgroundImg: smallImage11,
        userImage: avatar3,
        name: "Frank Hook",
        designation: "Project Manager",
        projectCount: 162,
        taskCount: 192
    },
    {
        id: 4,
        backgroundImg: smallImage1,
        userImage: avatar8,
        name: "Jennifer Carter",
        designation: "UI/UX Designer",
        projectCount: 241,
        taskCount: 205

    },
    {
        id: 5,
        backgroundImg: smallImage10,
        userImage: null,
        userShortName: "ME",
        name: "Megan Elmore",
        designation: "Team Leader & Web Developer",
        projectCount: 201,
        taskCount: 263,
        bgColor: "success",
        textColor: "success",

    },
    {
        id: 6,
        backgroundImg: smallImage2,
        userImage: avatar4,
        name: "Alexis Clarke",
        designation: "Backend Developer",
        projectCount: 132,
        taskCount: 147

    },
    {
        id: 7,
        backgroundImg: smallImage4,
        userImage: null,
        userShortName: "NC",
        name: "Nathan Cole",
        designation: "Front-End Developer",
        projectCount: 352,
        taskCount: 376,
        bgColor: "info",
        textColor: "info",
    },
    {
        id: 8,
        backgroundImg: smallImage7,
        userImage: avatar6,
        name: "Joseph Parker",
        designation: "Full Stack Developer",
        projectCount: 64,
        taskCount: 93
    },
    {
        id: 9,
        backgroundImg: smallImage3,
        userImage: avatar5,
        name: "Erica Kernan",
        designation: "Web Designer",
        projectCount: 345,
        taskCount: 298
    },
    {
        id: 10,
        backgroundImg: smallImage5,
        userImage: null,
        userShortName: "DP",
        name: "Donald Palmer",
        designation: "Wed Developer",
        projectCount: 95,
        taskCount: 135,
        bgColor: "light",
        textColor: "primary",

    },
    {
        id: 11,
        backgroundImg: smallImage8,
        userImage: avatar7,
        name: "Jack Gough",
        designation: "React Js Developer",
        projectCount: 87,
        taskCount: 121

    },
    {
        id: 12,
        backgroundImg: smallImage6,
        userImage: null,
        userShortName: "MW",
        name: "Marie Ward",
        designation: "Backend Developer",
        projectCount: 145,
        taskCount: 210,
        bgColor: "success",
        textColor: "success",

    },

];


export { gallery, pricing1, pricing2, pricing3, projects, document, SearchGallery, news, video, swiper, team };