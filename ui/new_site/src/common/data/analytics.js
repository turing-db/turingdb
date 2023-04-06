const topPages = [
    {
        id: 1,
        page: "/themesbrand/skote-25867",
        active: "99",
        user: "25.3%"
    },
    {
        id: 2,
        page: "/dashonic/chat-24518",
        active: "86",
        user: "22.7%"
    },
    {
        id: 3,
        page: "/skote/timeline-27391",
        active: "64",
        user: "18.7%"
    },
    {
        id: 4,
        page: "/themesbrand/minia-26441",
        active: "53",
        user: "14.2%"
    },
    {
        id: 5,
        page: "/dashon/dashboard-29873",
        active: "33",
        user: "12.6%"
    },
    {
        id: 6,
        page: "/doot/chats-29964",
        active: "20",
        user: "10.9%"
    },
    {
        id: 7,
        page: "/minton/pages-29739",
        active: "10",
        user: "07.3%"
    },
];

// Charts Data

const allData = [{
    data: [1230, 1240, 590, 1255, 1250, 789, 960, 522, 1285, 729],
    name: 'Sessions',
}];

const monthData = [{
    data: [820, 1320, 390, 1000, 850, 579, 630, 380, 825, 479],
    name: 'Sessions',
}];

const halfyearData = [{
    data: [1010, 1140, 1290, 1255, 1050, 689, 800, 420, 1085, 589],
    name: 'Sessions',
}];


// Audiences Metrics
const allaudiencesMetricsData = [{
    name: 'Last Year',
    data: [25.3, 12.5, 20.2, 18.5, 40.4, 25.4, 15.8, 22.3, 19.2, 25.3, 12.5, 20.2]
}, {
    name: 'Current Year',
    data: [36.2, 22.4, 38.2, 30.5, 26.4, 30.4, 20.2, 29.6, 10.9, 36.2, 22.4, 38.2]
}];

const monthaudiencesMetricsData = [{
    name: 'Last Year',
    data: [15.3, 22.5, 40.2, 78.5, 20.4, 35.4, 45.8, 32.3, 43.2, 35.3, 22.5, 10.2]
}, {
    name: 'Current Year',
    data: [26.2, 62.4, 48.2, 10.5, 26.4, 50.4, 40.2, 39.6, 10.9, 56.2, 12.4, 28.2]
}];

const halfyearaudiencesMetricsData = [{
    name: 'Last Year',
    data: [35.3, 12.5, 20.2, 28.5, 40.4, 25.4, 25.8, 32.3, 39.2, 35.3, 52.5, 40.2]
}, {
    name: 'Current Year',
    data: [36.2, 42.4, 48.2, 30.5, 26.4, 40.4, 60.2, 79.6, 30.9, 46.2, 32.4, 58.2]
}];

const yaeraudiencesMetricsData = [{
    name: 'Last Year',
    data: [45.3, 32.5, 40.2, 18.5, 40.4, 25.4, 35.8, 52.3, 79.2, 35.3, 72.5, 70.2]
}, {
    name: 'Current Year',
    data: [76.2, 62.4, 28.2, 30.5, 26.4, 60.4, 40.2, 39.6, 40.9, 76.2, 32.4, 98.2]
}];


// Users by Device
const todayDeviceData = [78.56, 105.02, 42.89];

const lastWeekDeviceData = [81.56, 50.02, 24.89];

const lastMonthDeviceData = [28.56, 75.02, 55.89];

const currentYearDeviceData = [20.56, 89.02, 35.89];

// Audiences Sessions by Country

function generateData(count, yrange) {
    var i = 0;
    var series = [];
    while (i < count) {
        var x = (i + 1).toString() + "h";
        var y = Math.floor(Math.random() * (yrange.max - yrange.min + 1)) + yrange.min;

        series.push({
            x: x,
            y: y
        });
        i++;
    }
    return series;
}

const todayaudiencesCountryData = [{
    name: 'Sat',
    data: generateData(18, {
        min: 0,
        max: 90
    })
},
{
    name: 'Fri',
    data: generateData(18, {
        min: 0,
        max: 90
    })
},
{
    name: 'Thu',
    data: generateData(18, {
        min: 0,
        max: 90
    })
},
{
    name: 'Wed',
    data: generateData(18, {
        min: 0,
        max: 90
    })
},
{
    name: 'Tue',
    data: generateData(18, {
        min: 0,
        max: 90
    })
},
{
    name: 'Mon',
    data: generateData(18, {
        min: 0,
        max: 90
    })
},
{
    name: 'Sun',
    data: generateData(18, {
        min: 0,
        max: 90
    })
}
];

const lastWeekaudiencesCountryData = [{
    name: 'Sat',
    data: generateData(18, {
        min: 10,
        max: 90
    })
},
{
    name: 'Fri',
    data: generateData(18, {
        min: 10,
        max: 90
    })
},
{
    name: 'Thu',
    data: generateData(18, {
        min: 10,
        max: 90
    })
},
{
    name: 'Wed',
    data: generateData(18, {
        min: 10,
        max: 90
    })
},
{
    name: 'Tue',
    data: generateData(18, {
        min: 10,
        max: 90
    })
},
{
    name: 'Mon',
    data: generateData(18, {
        min: 10,
        max: 90
    })
},
{
    name: 'Sun',
    data: generateData(18, {
        min: 10,
        max: 90
    })
}
];

const lastMonthaudiencesCountryData = [{
    name: 'Sat',
    data: generateData(18, {
        min: 0,
        max: 30
    })
},
{
    name: 'Fri',
    data: generateData(18, {
        min: 0,
        max: 30
    })
},
{
    name: 'Thu',
    data: generateData(18, {
        min: 0,
        max: 30
    })
},
{
    name: 'Wed',
    data: generateData(18, {
        min: 0,
        max: 30
    })
},
{
    name: 'Tue',
    data: generateData(18, {
        min: 0,
        max: 30
    })
},
{
    name: 'Mon',
    data: generateData(18, {
        min: 0,
        max: 30
    })
},
{
    name: 'Sun',
    data: generateData(18, {
        min: 0,
        max: 30
    })
}
];

const currentyearaudiencesCountryData = [{
    name: 'Sat',
    data: generateData(18, {
        min: 30,
        max: 70
    })
},
{
    name: 'Fri',
    data: generateData(18, {
        min: 30,
        max: 70
    })
},
{
    name: 'Thu',
    data: generateData(18, {
        min: 30,
        max: 70
    })
},
{
    name: 'Wed',
    data: generateData(18, {
        min: 30,
        max: 70
    })
},
{
    name: 'Tue',
    data: generateData(18, {
        min: 30,
        max: 70
    })
},
{
    name: 'Mon',
    data: generateData(18, {
        min: 30,
        max: 70
    })
},
{
    name: 'Sun',
    data: generateData(18, {
        min: 30,
        max: 70
    })
}
];



export { topPages, allData, monthData, halfyearData, allaudiencesMetricsData, monthaudiencesMetricsData, halfyearaudiencesMetricsData, yaeraudiencesMetricsData, todayDeviceData, lastWeekDeviceData, lastMonthDeviceData, currentYearDeviceData, todayaudiencesCountryData, lastWeekaudiencesCountryData, lastMonthaudiencesCountryData, currentyearaudiencesCountryData };