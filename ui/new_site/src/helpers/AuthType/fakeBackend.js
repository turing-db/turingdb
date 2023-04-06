import axios from "axios";
import MockAdapter from "axios-mock-adapter";
import * as url from "../url_helper";
import { accessToken, nodeApiToken } from "../jwt-token-access/accessToken";

import {
  calenderDefaultCategories,
  events,
  defaultevent,
  direactContact,
  messages,
  channelsList,
  projectList,
  sellersList,
  transactions,
  CryptoOrders,
  deals,
  mailbox,
  allData,
  monthData,
  halfyearData,
  allaudiencesMetricsData,
  monthaudiencesMetricsData,
  halfyearaudiencesMetricsData,
  yaeraudiencesMetricsData,
  todayDeviceData,
  lastWeekDeviceData,
  lastMonthDeviceData,
  currentYearDeviceData,
  todayBalanceData,
  lastWeekBalanceData,
  lastMonthBalanceData,
  currentYearBalanceData,
  todayDealData,
  weeklyDealData,
  monthlyDealData,
  yealyDealData,
  octData,
  novData,
  decData,
  janData,
  allRevenueData,
  monthRevenueData,
  halfYearRevenueData,
  yearRevenueData,
  btcPortfolioData,
  usdPortfolioData,
  euroPortfolioData,
  MarketGraphAll,
  MarketGraphYear,
  MarketGraphMonth,
  MarketGraphWeek,
  MarketGraphHour,
  allProjectData,
  monthProjectData,
  halfyearProjectData,
  yearProjectData,
  allTimeData,
  lastWeekData,
  lastMonthData,
  lastquarterData,
  allMarketplaceData,
  monthMarketplaceData,
  halfyearMarketplaceData,
  yearMarketplaceData,
  todayaudiencesCountryData,
  lastWeekaudiencesCountryData,
  lastMonthaudiencesCountryData,
  currentyearaudiencesCountryData,
  team,
  folderList,
  recentFile,
  todoTaskList,
  todoCollapse,
  jobApplication,
  apiKey
} from "../../common/data";

let users = [
  {
    uid: 1,
    username: "admin",
    role: "admin",
    password: "123456",
    email: "admin@themesbrand.com",
  },
];

const fakeBackend = () => {
  // This sets the mock adapter on the default instance
  const mock = new MockAdapter(axios, { onNoMatch: "passthrough" });

  mock.onPost("/post-jwt-register").reply(config => {
    const user = JSON.parse(config["data"]);
    users.push(user);

    return new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve([200, user]);
      });
    });
  });

  mock.onPost("/post-jwt-login").reply(config => {
    const user = JSON.parse(config["data"]);
    const validUser = users.filter(
      usr => usr.email === user.email && usr.password === user.password
    );

    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (validUser["length"] === 1) {
          // You have to generate AccessToken by jwt. but this is fakeBackend so, right now its dummy
          const token = accessToken;

          // JWT AccessToken
          const tokenObj = { accessToken: token }; // Token Obj
          const validUserObj = { ...validUser[0], ...tokenObj }; // validUser Obj

          resolve([200, validUserObj]);
        } else {
          reject([
            400,
            "Username and password are invalid. Please enter correct username and password",
          ]);
        }
      });
    });
  });

  mock.onPost("/post-jwt-profile").reply(config => {
    const user = JSON.parse(config["data"]);

    const one = config.headers;

    let finalToken = one.Authorization;

    const validUser = users.filter(usr => usr.uid === user.idx);

    return new Promise((resolve, reject) => {
      setTimeout(() => {
        // Verify Jwt token from header.Authorization
        if (finalToken === accessToken) {
          if (validUser["length"] === 1) {
            let objIndex;

            //Find index of specific object using findIndex method.
            objIndex = users.findIndex(obj => obj.uid === user.idx);

            //Update object's name property.
            users[objIndex].username = user.username;

            // Assign a value to locastorage
            sessionStorage.removeItem("authUser");
            sessionStorage.setItem("authUser", JSON.stringify(users[objIndex]));

            resolve([200, "Profile Updated Successfully"]);
          } else {
            reject([400, "Something wrong for edit profile"]);
          }
        } else {
          reject([400, "Invalid Token !!"]);
        }
      });
    });
  });

  mock.onPost("/social-login").reply(config => {
    const user = JSON.parse(config["data"]);
    return new Promise((resolve, reject) => {

      setTimeout(() => {
        if (user && user.token) {
          // You have to generate AccessToken by jwt. but this is fakeBackend so, right now its dummy
          const token = accessToken;
          const first_name = user.name
          const nodeapiToken = nodeApiToken
          delete user.name

          // JWT AccessToken
          const tokenObj = { accessToken: token, first_name: first_name }; // Token Obj
          const validUserObj = { token: nodeapiToken, "data": { ...tokenObj, ...user } }; // validUser Obj
          resolve([200, validUserObj]);
        } else {
          reject([
            400,
            "Username and password are invalid. Please enter correct username and password",
          ]);
        }
      });
    });
  });

  // Calendar
  mock.onGet(url.GET_EVENTS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (events) {
          // Passing fake JSON data as response
          const data = [...events, ...defaultevent];
          resolve([200, data]);
        } else {
          reject([400, "Cannot get events"]);
        }
      });
    });
  });

  mock.onGet(url.GET_CATEGORIES).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (calenderDefaultCategories) {
          // Passing fake JSON data as response
          resolve([200, calenderDefaultCategories]);
        } else {
          reject([400, "Cannot get categories"]);
        }
      });
    });
  });

  mock.onGet(url.GET_UPCOMMINGEVENT).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (defaultevent) {
          // Passing fake JSON data as response
          resolve([200, defaultevent]);
        } else {
          reject([400, "Cannot get upcomming events"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_EVENT).reply((event) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (event && event.data) {
          // Passing fake JSON data as response
          resolve([200, event.data]);
        } else {
          reject([400, "Cannot add event"]);
        }
      });
    });
  });

  mock.onPut(url.UPDATE_EVENT).reply((event) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (event && event.data) {
          // Passing fake JSON data as response
          resolve([200, event.data]);
        } else {
          reject([400, "Cannot update event"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_EVENT).reply((config) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.event]);
        } else {
          reject([400, "Cannot delete event"]);
        }
      });
    });
  });

  // Chat
  mock.onGet(url.GET_DIRECT_CONTACT).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (direactContact) {
          // Passing fake JSON data as response
          resolve([200, direactContact]);
        } else {
          reject([400, "Cannot get direct contact"]);
        }
      });
    });
  });

  mock.onGet(new RegExp(`${url.GET_MESSAGES}/*`)).reply(config => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (messages) {
          // Passing fake JSON data as response
          const { params } = config;
          const filteredMessages = messages.filter(
            msg => msg.roomId === params.roomId
          );

          resolve([200, filteredMessages]);
        } else {
          reject([400, "Cannot get messages"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_MESSAGE).reply(config => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config.data) {
          // Passing fake JSON data as response
          resolve([200, config.data]);
        } else {
          reject([400, "Cannot add message"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_MESSAGE).reply(config => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.message]);
        } else {
          reject([400, "Cannot delete message"]);
        }
      });
    });
  });

  mock.onGet(url.GET_CHANNELS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (channelsList) {
          // Passing fake JSON data as response
          resolve([200, channelsList]);
        } else {
          reject([400, "Cannot get Channels"]);
        }
      });
    });
  });

  // Project > List
  mock.onGet(url.GET_PROJECT_LIST).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (projectList) {
          // Passing fake JSON data as response
          resolve([200, projectList]);
        } else {
          reject([400, "Cannot get project list data"]);
        }
      });
    });
  });

  // MailBox
  mock.onGet(url.GET_MAIL_DETAILS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (mailbox) {
          // Passing fake JSON data as response
          resolve([200, mailbox]);
        } else {
          reject([400, "Cannot get mail details"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_MAIL).reply(config => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.forId]);
        } else {
          reject([400, "Cannot delete order"]);
        }
      });
    });
  });

  // Ecommerce > Seller
  mock.onGet(url.GET_SELLERS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (sellersList) {
          // Passing fake JSON data as response
          resolve([200, sellersList]);
        } else {
          reject([400, "Cannot get sellers"]);
        }
      });
    });
  });

  // Crypto > Transaction
  mock.onGet(url.GET_TRANSACTION_LIST).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (transactions) {
          // Passing fake JSON data as response
          resolve([200, transactions]);
        } else {
          reject([400, "Cannot get Transactions Data"]);
        }
      });
    });
  });

  // Crypto > Orders
  mock.onGet(url.GET_ORDRER_LIST).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (CryptoOrders) {
          // Passing fake JSON data as response
          resolve([200, CryptoOrders]);
        } else {
          reject([400, "Cannot get Order Data"]);
        }
      });
    });
  });

  // CRM > Deals
  mock.onGet(url.GET_DEALS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (deals) {
          // Passing fake JSON data as response
          resolve([200, deals]);
        } else {
          reject([400, "Cannot get Deals"]);
        }
      });
    });
  });

  // Dashborad Analytics
  // Sessions by Countries

  mock.onGet(url.GET_ALL_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (allData) {
          // Passing fake JSON data as response
          resolve([200, allData]);
        } else {
          reject([400, "Cannot get All Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHLY_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (monthData) {
          // Passing fake JSON data as response
          resolve([200, monthData]);
        } else {
          reject([400, "Cannot get Monthly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_HALFYEARLY_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (halfyearData) {
          // Passing fake JSON data as response
          resolve([200, halfyearData]);
        } else {
          reject([400, "Cannot get Half Yealy Chart Data"]);
        }
      });
    });
  });

  // Audiences Metrics
  mock.onGet(url.GET_ALLAUDIENCESMETRICS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (allaudiencesMetricsData) {
          // Passing fake JSON data as response
          resolve([200, allaudiencesMetricsData]);
        } else {
          reject([400, "Cannot get All Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHLYAUDIENCESMETRICS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (monthaudiencesMetricsData) {
          // Passing fake JSON data as response
          resolve([200, monthaudiencesMetricsData]);
        } else {
          reject([400, "Cannot get Monthly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_HALFYEARLYAUDIENCESMETRICS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (halfyearaudiencesMetricsData) {
          // Passing fake JSON data as response
          resolve([200, halfyearaudiencesMetricsData]);
        } else {
          reject([400, "Cannot get Half Yealy Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_YEARLYAUDIENCESMETRICS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (yaeraudiencesMetricsData) {
          // Passing fake JSON data as response
          resolve([200, yaeraudiencesMetricsData]);
        } else {
          reject([400, "Cannot get Yealy Chart Data"]);
        }
      });
    });
  });

  // Users by Device
  mock.onGet(url.GET_TODAYDEVICE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todayDeviceData) {
          // Passing fake JSON data as response
          resolve([200, todayDeviceData]);
        } else {
          reject([400, "Cannot get Today Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_LASTWEEKDEVICE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastWeekDeviceData) {
          // Passing fake JSON data as response
          resolve([200, lastWeekDeviceData]);
        } else {
          reject([400, "Cannot get Last Weekly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_LASTMONTHDEVICE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastMonthDeviceData) {
          // Passing fake JSON data as response
          resolve([200, lastMonthDeviceData]);
        } else {
          reject([400, "Cannot get Last Montly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_CURRENTYEARDEVICE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (currentYearDeviceData) {
          // Passing fake JSON data as response
          resolve([200, currentYearDeviceData]);
        } else {
          reject([400, "Cannot get Current Yealy Chart Data"]);
        }
      });
    });
  });

  // Audiences Sessions by Country

  mock.onGet(url.GET_TODAYSESSION_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todayaudiencesCountryData) {
          // Passing fake JSON data as response
          resolve([200, todayaudiencesCountryData]);
        } else {
          reject([400, "Cannot get Today Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_LASTWEEKSESSION_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastWeekaudiencesCountryData) {
          // Passing fake JSON data as response
          resolve([200, lastWeekaudiencesCountryData]);
        } else {
          reject([400, "Cannot get Last Weekly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_LASTMONTHSESSION_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastMonthaudiencesCountryData) {
          // Passing fake JSON data as response
          resolve([200, lastMonthaudiencesCountryData]);
        } else {
          reject([400, "Cannot get Last Montly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_CURRENTYEARSESSION_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (currentyearaudiencesCountryData) {
          // Passing fake JSON data as response
          resolve([200, currentyearaudiencesCountryData]);
        } else {
          reject([400, "Cannot get Current Yealy Chart Data"]);
        }
      });
    });
  });

  // Dashboard CRM

  // Balance Overview
  mock.onGet(url.GET_TODAYBALANCE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todayBalanceData) {
          // Passing fake JSON data as response
          resolve([200, todayBalanceData]);
        } else {
          reject([400, "Cannot get Today Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_LASTWEEKBALANCE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastWeekBalanceData) {
          // Passing fake JSON data as response
          resolve([200, lastWeekBalanceData]);
        } else {
          reject([400, "Cannot get Last Weekly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_LASTMONTHBALANCE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastMonthBalanceData) {
          // Passing fake JSON data as response
          resolve([200, lastMonthBalanceData]);
        } else {
          reject([400, "Cannot get Last Montly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_CURRENTYEARBALANCE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (currentYearBalanceData) {
          // Passing fake JSON data as response
          resolve([200, currentYearBalanceData]);
        } else {
          reject([400, "Cannot get Current Yealy Chart Data"]);
        }
      });
    });
  });

  // Deal Type
  mock.onGet(url.GET_TODAYDEAL_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todayDealData) {
          // Passing fake JSON data as response
          resolve([200, todayDealData]);
        } else {
          reject([400, "Cannot get Today Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_WEEKLYDEAL_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (weeklyDealData) {
          // Passing fake JSON data as response
          resolve([200, weeklyDealData]);
        } else {
          reject([400, "Cannot get Weekly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHLYDEAL_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (monthlyDealData) {
          // Passing fake JSON data as response
          resolve([200, monthlyDealData]);
        } else {
          reject([400, "Cannot get Montly Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_YEARLYDEAL_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (yealyDealData) {
          // Passing fake JSON data as response
          resolve([200, yealyDealData]);
        } else {
          reject([400, "Cannot get Yealy Chart Data"]);
        }
      });
    });
  });

  // Sales Forecast
  mock.onGet(url.GET_OCTSALES_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (octData) {
          // Passing fake JSON data as response
          resolve([200, octData]);
        } else {
          reject([400, "Cannot get October Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_NOVSALES_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (novData) {
          // Passing fake JSON data as response
          resolve([200, novData]);
        } else {
          reject([400, "Cannot get November Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_DECSALES_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (decData) {
          // Passing fake JSON data as response
          resolve([200, decData]);
        } else {
          reject([400, "Cannot get December Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_JANSALES_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (janData) {
          // Passing fake JSON data as response
          resolve([200, janData]);
        } else {
          reject([400, "Cannot get January Chart Data"]);
        }
      });
    });
  });

  // Dashboard Ecommerce
  // Revenue
  mock.onGet(url.GET_ALLREVENUE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (allRevenueData) {
          // Passing fake JSON data as response
          resolve([200, allRevenueData]);
        } else {
          reject([400, "Cannot get All Revenue Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHREVENUE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (monthRevenueData) {
          // Passing fake JSON data as response
          resolve([200, monthRevenueData]);
        } else {
          reject([400, "Cannot get Month Revenue Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_HALFYEARREVENUE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (halfYearRevenueData) {
          // Passing fake JSON data as response
          resolve([200, halfYearRevenueData]);
        } else {
          reject([400, "Cannot get Half Year Revenue Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_YEARREVENUE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (yearRevenueData) {
          // Passing fake JSON data as response
          resolve([200, yearRevenueData]);
        } else {
          reject([400, "Cannot get Year Revenue Data"]);
        }
      });
    });
  });

  // Dashboard Crypto
  // Portfolio
  mock.onGet(url.GET_BTCPORTFOLIO_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (btcPortfolioData) {
          // Passing fake JSON data as response
          resolve([200, btcPortfolioData]);
        } else {
          reject([400, "Cannot get BTC Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_USDPORTFOLIO_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (usdPortfolioData) {
          // Passing fake JSON data as response
          resolve([200, usdPortfolioData]);
        } else {
          reject([400, "Cannot get USD Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_EUROPORTFOLIO_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (euroPortfolioData) {
          // Passing fake JSON data as response
          resolve([200, euroPortfolioData]);
        } else {
          reject([400, "Cannot get EURO Data"]);
        }
      });
    });
  });

  // Market Graph
  mock.onGet(url.GET_ALLMARKETDATA_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (MarketGraphAll) {
          // Passing fake JSON data as response
          resolve([200, MarketGraphAll]);
        } else {
          reject([400, "Cannot get All Market Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_YEARMARKET_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (MarketGraphYear) {
          // Passing fake JSON data as response
          resolve([200, MarketGraphYear]);
        } else {
          reject([400, "Cannot get Year Market Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHMARKET_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (MarketGraphMonth) {
          // Passing fake JSON data as response
          resolve([200, MarketGraphMonth]);
        } else {
          reject([400, "Cannot get Month Market Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_WEEKMARKET_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (MarketGraphWeek) {
          // Passing fake JSON data as response
          resolve([200, MarketGraphWeek]);
        } else {
          reject([400, "Cannot get Week Market Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_HOURMARKET_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (MarketGraphHour) {
          // Passing fake JSON data as response
          resolve([200, MarketGraphHour]);
        } else {
          reject([400, "Cannot get Hour Market Data"]);
        }
      });
    });
  });

  // Dashboard Project
  // Project Overview
  mock.onGet(url.GET_ALLPROJECT_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (allProjectData) {
          // Passing fake JSON data as response
          resolve([200, allProjectData]);
        } else {
          reject([400, "Cannot get All Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHPROJECT_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (monthProjectData) {
          // Passing fake JSON data as response
          resolve([200, monthProjectData]);
        } else {
          reject([400, "Cannot get Month Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_HALFYEARPROJECT_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (halfyearProjectData) {
          // Passing fake JSON data as response
          resolve([200, halfyearProjectData]);
        } else {
          reject([400, "Cannot get Half Year Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_YEARPROJECT_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (yearProjectData) {
          // Passing fake JSON data as response
          resolve([200, yearProjectData]);
        } else {
          reject([400, "Cannot get Year Chart Data"]);
        }
      });
    });
  });

  // Project Status
  mock.onGet(url.GET_ALLPROJECTSTATUS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (allTimeData) {
          // Passing fake JSON data as response
          resolve([200, allTimeData]);
        } else {
          reject([400, "Cannot get All Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_WEEKPROJECTSTATUS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastWeekData) {
          // Passing fake JSON data as response
          resolve([200, lastWeekData]);
        } else {
          reject([400, "Cannot get Last Week Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHPROJECTSTATUS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastMonthData) {
          // Passing fake JSON data as response
          resolve([200, lastMonthData]);
        } else {
          reject([400, "Cannot get Last Month Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_QUARTERPROJECTSTATUS_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (lastquarterData) {
          // Passing fake JSON data as response
          resolve([200, lastquarterData]);
        } else {
          reject([400, "Cannot get Last Quarter Chart Data"]);
        }
      });
    });
  });

  // Dashboard NFT
  // Marketplace
  mock.onGet(url.GET_ALLMARKETPLACE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (allMarketplaceData) {
          // Passing fake JSON data as response
          resolve([200, allMarketplaceData]);
        } else {
          reject([400, "Cannot get All Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_MONTHMARKETPLACE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (monthMarketplaceData) {
          // Passing fake JSON data as response
          resolve([200, monthMarketplaceData]);
        } else {
          reject([400, "Cannot get Month Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_HALFYEARMARKETPLACE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (halfyearMarketplaceData) {
          // Passing fake JSON data as response
          resolve([200, halfyearMarketplaceData]);
        } else {
          reject([400, "Cannot get Half Year Chart Data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_YEARMARKETPLACE_DATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (yearMarketplaceData) {
          // Passing fake JSON data as response
          resolve([200, yearMarketplaceData]);
        } else {
          reject([400, "Cannot get Year Chart Data"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_PROJECT).reply((project) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (project && project.data) {
          // Passing fake JSON data as response
          resolve([200, project.data]);
        } else {
          reject([400, "Cannot add project"]);
        }
      });
    });
  });

  mock.onPut(url.UPDATE_PROJECT).reply((project) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (project && project.data) {
          // Passing fake JSON data as response
          resolve([200, project.data]);
        } else {
          reject([400, "Cannot update project"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_PROJECT).reply((config) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.project]);
        } else {
          reject([400, "Cannot delete event"]);
        }
      });
    });
  });

  mock.onGet(url.GET_TEAMDATA).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (team) {
          // Passing fake JSON data as response
          resolve([200, team]);
        } else {
          reject([400, "Cannot get team data"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_TEAMDATA).reply((config) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.team]);
        } else {
          reject([400, "Cannot delete team data"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_TEAMDATA).reply((team) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (team && team.data) {
          // Passing fake JSON data as response
          resolve([200, team.data]);
        } else {
          reject([400, "Cannot add team data"]);
        }
      });
    });
  });

  mock.onPut(url.UPDATE_TEAMDATA).reply((team) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (team && team.data) {
          // Passing fake JSON data as response
          resolve([200, team.data]);
        } else {
          reject([400, "Cannot update team data"]);
        }
      });
    });
  });

  // File Manager
  // Folder
  mock.onGet(url.GET_FOLDERS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (folderList) {
          // Passing fake JSON data as response
          resolve([200, folderList]);
        } else {
          reject([400, "Cannot get folder data"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_FOLDER).reply((config) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.folder]);
        } else {
          reject([400, "Cannot delete folder data"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_FOLDER).reply((folder) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (folder && folder.data) {
          // Passing fake JSON data as response
          resolve([200, folder.data]);
        } else {
          reject([400, "Cannot add folder data"]);
        }
      });
    });
  });

  mock.onPut(url.UPDATE_FOLDER).reply((folder) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (folder && folder.data) {
          // Passing fake JSON data as response
          resolve([200, folder.data]);
        } else {
          reject([400, "Cannot update folder data"]);
        }
      });
    });
  });

  // File
  mock.onGet(url.GET_FILES).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (recentFile) {
          // Passing fake JSON data as response
          resolve([200, recentFile]);
        } else {
          reject([400, "Cannot get file data"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_FILE).reply((config) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.file]);
        } else {
          reject([400, "Cannot delete file data"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_FILE).reply((file) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (file && file.data) {
          // Passing fake JSON data as response
          resolve([200, file.data]);
        } else {
          reject([400, "Cannot add file data"]);
        }
      });
    });
  });

  mock.onPut(url.UPDATE_FILE).reply((file) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (file && file.data) {
          // Passing fake JSON data as response
          resolve([200, file.data]);
        } else {
          reject([400, "Cannot update file data"]);
        }
      });
    });
  });

  // To do
  mock.onGet(url.GET_TODOS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todoTaskList) {
          // Passing fake JSON data as response
          resolve([200, todoTaskList]);
        } else {
          reject([400, "Cannot get To do data"]);
        }
      });
    });
  });

  mock.onDelete(url.DELETE_TODO).reply((config) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (config && config.headers) {
          // Passing fake JSON data as response
          resolve([200, config.headers.todo]);
        } else {
          reject([400, "Cannot delete To do data"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_TODO).reply((todo) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todo && todo.data) {
          // Passing fake JSON data as response
          resolve([200, todo.data]);
        } else {
          reject([400, "Cannot add To do data"]);
        }
      });
    });
  });

  mock.onPut(url.UPDATE_TODO).reply((todo) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todo && todo.data) {
          // Passing fake JSON data as response
          resolve([200, todo.data]);
        } else {
          reject([400, "Cannot update To do data"]);
        }
      });
    });
  });

  mock.onGet(url.GET_PROJECTS).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (todoCollapse) {
          // Passing fake JSON data as response
          resolve([200, todoCollapse]);
        } else {
          reject([400, "Cannot get Project data"]);
        }
      });
    });
  });

  mock.onPost(url.ADD_NEW_TODO_PROJECT).reply((project) => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (project && project.data) {
          // Passing fake JSON data as response
          resolve([200, project.data]);
        } else {
          reject([400, "Cannot add Project data"]);
        }
      });
    });
  });

  //JOBS
  mock.onGet(url.GET_APPLICATION_LIST).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (jobApplication) {
          // Passing fake JSON data as response
          resolve([200, jobApplication]);
        } else {
          reject([400, "Cannot get Application Data"]);
        }
      });
    });
  });

  //API Key
  mock.onGet(url.GET_API_KEY).reply(() => {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (apiKey) {
          // Passing fake JSON data as response
          resolve([200, apiKey]);
        } else {
          reject([400, "Cannot get API Key Data"]);
        }
      });
    });
  });

};

export default fakeBackend;