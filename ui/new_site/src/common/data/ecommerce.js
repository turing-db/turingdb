/** Product 1 */
import product2 from "../../assets/images/products/img-2.png";
import product3 from "../../assets/images/products/img-3.png";
import product7 from "../../assets/images/products/img-7.png";
import product8 from "../../assets/images/products/img-8.png";

import img10 from "../../assets/images/small/img-10.jpg";
import img11 from "../../assets/images/small/img-11.jpg";
import img12 from "../../assets/images/small/img-12.jpg";

import company1 from "../../assets/images/companies/img-1.png";
import company2 from "../../assets/images/companies/img-2.png";
import company3 from "../../assets/images/companies/img-3.png";
import company4 from "../../assets/images/companies/img-4.png";
import company5 from "../../assets/images/companies/img-5.png";
import company6 from "../../assets/images/companies/img-6.png";
import company7 from "../../assets/images/companies/img-7.png";
import company8 from "../../assets/images/companies/img-8.png";

const productsData = [
  {
    id: 1,
    image: "img-1.png",
    name: "Half Sleeve Round Neck T-Shirts",
    category: "Clothes",
    stock: "12",
    price: 115,
    orders: "48",
    type: "published",
    rating: 4.2,
    publishedDate: ["12 Oct, 2021", "10:05 AM"],
    categories: "fashion"
  },
  {
    id: 2,
    image: "img-2.png",
    name: "Urban Ladder Pashe Chair",
    category: "Furniture",
    stock: "06",
    price: 160,
    orders: "30",
    type: "unpublished",
    rating: 3.3,
    publishedDate: ["06 Jan, 2021", "01:31 PM"],
    categories: "furniture"
  },
  {
    id: 3,
    image: "img-3.png",
    name: "350 ml Glass Grocery Container",
    category: "Kitchen Storage & Containers",
    stock: "10",
    price: 25,
    orders: "48",
    type: "unpublished",
    rating: 4.5,
    publishedDate: ["26 Mar, 2021", "11:40 AM"],
    categories: "grocery"
  },
  {
    id: 4,
    image: "img-4.png",
    name: "Fabric Dual Tone Living Room Chair",
    category: "Furniture",
    stock: "15",
    price: 140,
    orders: "40",
    type: "unpublished",
    rating: 3.2,
    publishedDate: ["19 Apr, 2021", "02:51 PM"],
    categories: "furniture"
  },
  {
    id: 5,
    image: "img-5.png",
    name: "Crux Motorsports Helmet",
    category: "Bike Accessories",
    stock: "08",
    price: 135,
    orders: "55",
    type: "unpublished",
    rating: 4.4,
    publishedDate: ["30 Mar, 2021", "09:42 AM"],
    categories: "auto"
  },
  {
    id: 6,
    image: "img-6.png",
    name: "Half Sleeve T-Shirts (Blue)",
    category: "Clothes",
    stock: "15",
    price: 125,
    orders: "48",
    type: "unpublished",
    rating: 3.2,
    publishedDate: ["12 Oct, 2021", "04:55 PM"],
    categories: "fashion"
  },
  {
    id: 7,
    image: "img-7.png",
    name: "Noise Evolve Smartwatch",
    category: "Watches",
    stock: "12",
    price: 95,
    orders: "45",
    type: "published",
    rating: 4.3,
    publishedDate: ["15 May, 2021", "03:40 PM"],
    categories: "watches"
  },
  {
    id: 8,
    image: "img-8.png",
    name: "Sweatshirt for Men (Pink)",
    category: "Clothes",
    stock: "20",
    price: 120,
    orders: "48",
    type: "published",
    rating: 2.2,
    publishedDate: ["21 Jun, 2021", "12:18 PM"],
    categories: "fashion"
  },
  {
    id: 9,
    image: "img-9.png",
    name: "Reusable Ecological Coffee Cup",
    category: "Tableware & Dinnerware",
    stock: "14",
    price: 125,
    orders: "55",
    type: "published",
    rating: 2.3,
    publishedDate: ["15 Jan, 2021", "10:29 AM"],
    categories: "grocery"
  },
  {
    id: 10,
    image: "img-10.png",
    name: "Travel Carrying Pouch Bag",
    category: "Bags, Wallets and Luggage",
    stock: "20",
    price: 115,
    orders: "60",
    type: "published",
    rating: 2.3,
    publishedDate: ["15 Jun, 2021", "03:51 Pm"],
    categories: "kids"
  },
  {
    id: 11,
    image: "img-1.png",
    name: "Half Sleeve Round Neck T-Shirts",
    category: "Clothes",
    stock: "12",
    price: 115,
    orders: "48",
    type: "published",
    rating: 4.2,
    publishedDate: ["12 Oct, 2021", "10:05 AM"],
    categories: "fashion"
  },
  {
    id: 12,
    image: "img-2.png",
    name: "Urban Ladder Pashe Chair",
    category: "Furniture",
    stock: "06",
    price: 160,
    orders: "30",
    type: "published",
    rating: 4.3,
    publishedDate: ["06 Jan, 2021", "01:31 PM"],
    categories: "furniture"
  },
];

// Ecommerce > Product Details
const productDetailsWidgets = [
  {
    id: 1,
    icon: "ri-money-dollar-circle-fill",
    label: "Price",
    labelDetail: "$120.40",
  },
  {
    id: 2,
    icon: "ri-file-copy-2-fill",
    label: "No. of Orders",
    labelDetail: "2,234",
  },
  {
    id: 3,
    icon: "ri-stack-fill",
    label: "Available Stocks",
    labelDetail: "1,230",
  },
  {
    id: 4,
    icon: "ri-inbox-archive-fill",
    label: "Total Revenue",
    labelDetail: "$60,645",
  },
];

const reviews = [
  {
    id: 1,
    rating: "4.2",
    comment: "Superb sweatshirt. I loved it. It is for winter.",
    name: "Henry",
    date: "12 Jul, 21",
    subitem: [
      { id: 1, img: img12 },
      { id: 2, img: img11 },
      { id: 3, img: img10 },
    ],
  },
  {
    id: 2,
    rating: "4.0",
    comment: "Great at this price, Product quality and look is awesome.",
    name: "Nancy",
    date: "06 Jul, 21",
  },
  {
    id: 3,
    rating: "4.2",
    comment: "Good product. I am so happy.",
    name: "Joseph",
    date: "06 Jul, 21",
  },
  {
    id: 4,
    rating: "4.1",
    comment: "Nice Product, Good Quality.",
    name: "Jimmy",
    date: "24 Jun, 21",
  },
];

// Ecommerce > Orders

const orders = [
  {
    id: 1,
    orderId: "#VZ2101",
    customer: "Frank Hook",
    productname: "Puma Tshirt",
    orderDate: "20 Dec, 2021, 02:21 AM",
    // orderDate: ["20 Dec, 2021", "02:21 AM"],
    // ordertime: "02:21 AM",
    amount: "$654",
    payment: "Mastercard",
    status: "Pending",
    statusClass: "warning",
  },
  {
    id: 2,
    orderId: "#VZ2102",
    customer: "Rickey Teran",
    productname: "Adidas Sneakers",
    orderDate: "16 Dec, 2021, 03:41 PM",
    // orderDate: ["16 Dec, 2021", "03:41 PM"],
    // ordertime: "03:41 PM",
    amount: "$354",
    payment: "Paypal",
    status: "Cancelled",
    statusClass: "danger",
  },
  {
    id: 3,
    orderId: "#VZ2103",
    customer: "James Price",
    productname: "350 ml Glass Grocery Container",
    orderDate: "28 Nov, 2021, 11:33 AM",
    // orderDate: ["28 Nov, 2021", "11:33 AM"],
    // ordertime: "11:33 AM",
    amount: "$829",
    payment: "Visa",
    status: "Inprogress",
    statusClass: "secondary",
  },
  {
    id: 4,
    orderId: "#VZ2104",
    customer: "Nettie Deloatch",
    productname: "American egale outfitters Shirt",
    orderDate: "22 Nov, 2021, 10:19 PM",
    // orderDate: ["22 Nov, 2021", "10:19 PM"],
    // ordertime: "10:19 PM",
    amount: "$142",
    payment: "COD",
    status: "Pending",
    statusClass: "warning",
  },
  {
    id: 5,
    orderId: "#VZ2105",
    customer: "Thomas Taylor",
    productname: "Galaxy Watch4",
    orderDate: "12 Nov, 2021, 11:47 AM",
    // orderDate: ["12 Nov, 2021", "11:47 AM"],
    // ordertime: "11:47 AM",
    amount: "$408",
    payment: "Mastercard",
    status: "Pickups",
    statusClass: "info",
  },
  {
    id: 6,
    orderId: "#VZ2106",
    customer: "James Price",
    productname: "Apple iPhone 12",
    orderDate: "05 Nov, 2021, 11:47 AM",
    // orderDate: ["05 Nov, 2021", "11:47 AM"],
    // ordertime: "11:47 AM",
    amount: "$1240",
    payment: "Visa",
    status: "Inprogress",
    statusClass: "secondary",
  },
  {
    id: 7,
    orderId: "#VZ2107",
    customer: "Nancy Martino",
    productname: "Funky Prints T-shirt",
    orderDate: "31 Oct, 2021, 08:55 PM",
    // orderDate: ["31 Oct, 2021", "08:55 PM"],
    // ordertime: "08:55 PM",
    amount: "$180",
    payment: "COD",
    status: "Returns",
    statusClass: "primary",
  },
  {
    id: 8,
    orderId: "#VZ2108",
    customer: "Alexis Clarke",
    productname: "USB Flash Drive Personalized with 3D Print",
    orderDate: "25 Oct, 2021, 05:33 AM",
    // orderDate: ["25 Oct, 2021", "05:33 AM"],
    // ordertime: "05:33 AM",
    amount: "$247",
    payment: "Paypal",
    status: "Delivered",
    statusClass: "success",
  },
  {
    id: 9,
    orderId: "#VZ2109",
    customer: "Donald Palmer",
    productname: "Oxford Button-Down Shirt",
    orderDate: "19 Oct, 2021, 12:43 AM",
    // orderDate: ["19 Oct, 2021", "12:43 AM"],
    // ordertime: "12:43 AM",
    amount: "$373",
    payment: "Visa",
    status: "Pickups",
    statusClass: "info",
  },
  {
    id: 10,
    orderId: "#VZ2110",
    customer: "Henry Baird",
    productname: "Classic Short Sleeve Shirt",
    orderDate: "13 Oct, 2021, 01:20 PM",
    // orderDate: ["13 Oct, 2021", "01:20 PM"],
    // ordertime: "01:20 PM",
    amount: "$342",
    payment: "Mastercard",
    status: "Inprogress",
    statusClass: "secondary",
  },
  {
    id: 11,
    orderId: "#VZ2111",
    customer: "Diana Kohler",
    productname: "Half Sleeve T-Shirts (Blue)",
    orderDate: "01 Oct, 2021, 03:08 AM",
    // orderDate: ["01 Oct, 2021", "03:08 AM"],
    // ordertime: "03:08 AM",
    amount: "$874",
    payment: "Visa",
    status: "Delivered",
    statusClass: "success",
  },
  {
    id: 12,
    orderId: "#VZ2112",
    customer: "Alexis Clarke",
    productname: "Noise Evolve Smartwatch",
    orderDate: "29 Sep, 2021, 04:24 AM",
    // orderDate: ["29 Sep, 2021", "04:24 AM"],
    // ordertime: "04:24 AM",
    amount: "$1021",
    payment: "Mastercard",
    status: "Cancelled",
    statusClass: "danger",
  },
];

// Ecommerce > Order Details

const productDetails = [
  {
    id: 1,
    img: product8,
    name: "Sweatshirt for Men (Pink)",
    color: "Pink",
    size: "M",
    price: "$119.99",
    quantity: "02",
    amount: "$239.98",
  },
  {
    id: 2,
    img: product7,
    name: "Noise NoiseFit Endure Smart Watch",
    color: "Black",
    size: "32.5mm",
    price: "$94.99",
    quantity: "01",
    amount: "$94.99",
  },
  {
    id: 3,
    img: product3,
    name: "350 ml Glass Grocery Container",
    color: "White",
    size: "350 ml",
    price: "$24.99",
    quantity: "01",
    amount: "$24.99",
  },
];

// Ecommerce > Customers

const customerList = [
  {
    id: 1,
    customerId: "#VZ2101",
    customer: "Mary Cousar",
    email: "marycousar@velzon.com",
    phone: "580-464-4694",
    date: "06 Apr, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 2,
    customerId: "#VZ2102",
    customer: "Jeff Taylor",
    email: "jefftaylor@velzon.com",
    phone: "863-577-5537",
    date: "15 Feb, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 3,
    customerId: "#VZ2103",
    customer: "Robert McMahon",
    email: "robertmcmahon@velzon.com",
    phone: "786-253-9927",
    date: "12 Jan, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 4,
    customerId: "#VZ2104",
    customer: "Michael Morris",
    email: "michaelmorris@velzon.com",
    phone: "805-447-8398",
    date: "19 May, 2021",
    status: "Block",
    statusClass: "danger",
  },
  {
    id: 5,
    customerId: "#VZ2105",
    customer: "Kevin Dawson",
    email: "kevindawson@velzon.com",
    phone: "213-741-4294",
    date: "14 Apr, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 6,
    customerId: "#VZ2106",
    customer: "Carolyn Jones",
    email: "carolynjones@velzon.com",
    phone: "414-453-5725",
    date: "07 Jun, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 7,
    customerId: "#VZ2107",
    customer: "Glen Matney",
    email: "glenmatney@velzon.com",
    phone: "515-395-1069",
    date: "02 Nov, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 8,
    customerId: "#VZ2108",
    customer: "Charles Kubik",
    email: "charleskubik@velzon.com",
    phone: "231-480-8536",
    date: "25 Sep, 2021",
    status: "Block",
    statusClass: "danger",
  },
  {
    id: 9,
    customerId: "#VZ2109",
    customer: "Herbert Stokes",
    email: "herbertstokes@velzon.com",
    phone: "312-944-1448",
    date: "20 Jul, 2021",
    status: "Block",
    statusClass: "danger",
  },
  {
    id: 10,
    customerId: "#VZ21010",
    customer: "Timothy Smith",
    email: "timothysmith@velzon.com",
    phone: "973-277-6950",
    date: "13 Dec, 2021",
    status: "Active",
    statusClass: "success",
  },
  {
    id: 11,
    customerId: "#VZ21011",
    customer: "Johnny Evans",
    email: "johnnyevans@velzon.com",
    phone: "407-645-1767",
    date: "01 Oct, 2021",
    status: "Block",
    statusClass: "danger",
  },
  {
    id: 12,
    customerId: "#VZ21012",
    customer: "Kevin Dawson",
    email: "kevindawson@velzon.com",
    phone: "213-741-4294",
    date: "14 Apr, 2021",
    status: "Active",
    statusClass: "success",
  },
];

// Ecommerce > Shopping Cart

const shoppingCart = [
  {
    id: 1,
    img: product8,
    name: "Sweatshirt for Men (Pink)",
    color: "Pink",
    size: "M",
    data_attr: 1,
    price: 119.99,
    total: 119.99,
  },
  {
    id: 2,
    img: product7,
    name: "Noise NoiseFit Endure Smart Watch",
    color: "Black",
    size: "32.5mm",
    data_attr: 1,
    price: 94.99,
    total: 94.99,
  },
  {
    id: 3,
    img: product3,
    name: "350 ml Glass Grocery Container",
    color: "White",
    size: "350 ml",
    data_attr: 1,
    price: 24.99,
    total: 24.99,
  },
];

// Ecommerce > Checkout

const orderSummary = [
  {
    id: 1,
    img: product8,
    name: "Sweatshirt for Men (Pink)",
    price: 119.99,
    quantity: 2,
    total: 239.98
  },
  {
    id: 2,
    img: product7,
    name: "Noise Evolve Smartwatch",
    price: 94.99,
    quantity: 1,
    total: 94.99
  },
  {
    id: 3,
    img: product3,
    name: "350 ml Glass Grocery Container",
    price: 24.99,
    quantity: 1,
    total: 24.99
  },
];

// Ecommerce > Sellers

const sellersList = [
  {
    id: 1,
    isTrending: true,
    img: company1,
    label: "Force Medicines",
    name: "David Marshall",
    stock: "452",
    chartdata: [12, 14, 2, 47, 42, 15, 47, 75, 65, 19, 14],
    color: "rgba(37,160,226, 0.65)",
    balance: "$45,415",
    category: "Health & Medicine"
  },
  {
    id: 2,
    isTrending: false,
    img: company2,
    label: "Micro Design",
    name: "Katia Stapleton",
    stock: "784",
    chartdata: [12, 14, 2, 47, 42, 15, 35, 75, 20, 67, 89],
    color: "rgba(37,160,226, 0.65)",
    balance: "$97,642",
    category: "Manufacturer"
  },
  {
    id: 3,
    isTrending: false,
    img: company3,
    label: "Nesta Technologies",
    name: "Harley Fuller",
    stock: "320",
    chartdata: [45, 20, 8, 42, 30, 5, 35, 79, 22, 54, 64],
    color: "rgba(37,160,226, 0.65)",
    balance: "$27,102",
    category: "Manufacturer"
  },
  {
    id: 4,
    isTrending: true,
    img: company4,
    label: "iTest Factory",
    name: "Oliver Tyler",
    stock: "159",
    chartdata: [26, 15, 48, 12, 47, 19, 35, 19, 85, 68, 50],
    color: "rgba(37,160,226, 0.65)",
    balance: "$14,933",
    category: "Food Service"
  },
  {
    id: 5,
    isTrending: false,
    img: company5,
    label: "Meta4Systems",
    name: "Zoe Dennis",
    stock: "363",
    chartdata: [60, 67, 12, 49, 6, 78, 63, 51, 33, 8, 16],
    color: "rgba(37,160,226, 0.65)",
    balance: "$73,426",
    category: "Computers & Electronics"
  },
  {
    id: 6,
    isTrending: true,
    img: company6,
    label: "Digitech Galaxy",
    name: "John Roberts",
    stock: "412",
    chartdata: [78, 63, 51, 33, 8, 16, 60, 67, 12, 49],
    color: "rgba(37,160,226, 0.65)",
    balance: "$34,241",
    category: "Computers & Electronics"
  },
  {
    id: 7,
    isTrending: true,
    img: company7,
    label: "Syntyce Solutions",
    name: "Demi Allen",
    stock: "945",
    chartdata: [15, 35, 75, 20, 67, 8, 42, 30, 5, 35],
    color: "rgba(37,160,226, 0.65)",
    balance: "$17,200",
    category: "Manufacturer"
  },
  {
    id: 8,
    isTrending: false,
    img: company8,
    label: "Zoetic Fashion",
    name: "James Bowen",
    stock: "784",
    chartdata: [45, 32, 68, 55, 36, 10, 48, 25, 74, 54],
    color: "rgba(37,160,226, 0.65)",
    balance: "$97,642",
    category: "Retailer"
  },
];

// Ecommerce > Seller Details > Revenue

const revenueWidgets = [
  {
    id: 1,
    counter: "7585",
    label: "Orders",
    decimals: 0,
    prefix: "",
    separator: "",
    suffix: ""
  },
  {
    id: 2,
    counter: "22.89",
    label: "Earnings",
    decimals: 2,
    prefix: "$",
    separator: ",",
    suffix: "k"
  },
  {
    id: 3,
    counter: "367",
    label: "Refunds",
    decimals: 1,
    prefix: "$",
    separator: ",",
    suffix: ""
  },
  {
    id: 4,
    counter: "18.92",
    counterClass: "text-success",
    label: "Conversation Ratio",
    decimals: 2,
    prefix: "",
    separator: "",
    suffix: "%"
  },
];


// Ecommerce > Seller Details > Products Review

const productsReview = [
  {
    id: 1,
    img: company1,
    caption: "Great product and looks great, lots of features.",
    name: "Force Medicines",
    subItem: [
      { id: 1, icon: "ri-star-fill" },
      { id: 2, icon: "ri-star-fill" },
      { id: 3, icon: "ri-star-fill" },
      { id: 4, icon: "ri-star-fill" },
      { id: 5, icon: "ri-star-fill" },
    ],
  },
  {
    id: 2,
    img: product3,
    caption: "Amazing template, very easy to understand and manipulate.",
    name: "Henry Baird",
    subItem: [
      { id: 1, icon: "ri-star-fill" },
      { id: 2, icon: "ri-star-fill" },
      { id: 3, icon: "ri-star-fill" },
      { id: 4, icon: "ri-star-fill" },
      { id: 5, icon: "ri-star-half-fill" },
    ],
  },
  {
    id: 3,
    img: company8,
    caption: "Very beautiful product and Very helpful customer service.",
    name: "Zoetic Fashion",
    subItem: [
      { id: 1, icon: "ri-star-fill" },
      { id: 2, icon: "ri-star-fill" },
      { id: 3, icon: "ri-star-fill" },
      { id: 4, icon: "ri-star-line" },
      { id: 5, icon: "ri-star-line" },
    ],
  },
  {
    id: 4,
    img: product2,
    caption: "The product is very beautiful. I like it.",
    name: "Nancy Martino",
    subItem: [
      { id: 1, icon: "ri-star-fill" },
      { id: 2, icon: "ri-star-fill" },
      { id: 3, icon: "ri-star-fill" },
      { id: 4, icon: "ri-star-half-fill" },
      { id: 5, icon: "ri-star-line" },
    ],
  },
];

export { productsData, productDetailsWidgets, reviews, orders, productDetails, customerList, shoppingCart, orderSummary, sellersList, revenueWidgets, productsReview };