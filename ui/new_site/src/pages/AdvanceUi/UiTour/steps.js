const steps = [
  {
    id: "intro",
    attachTo: { element: "#logo-tour", on: "bottom" },
    beforeShowPromise: function () {
      return new Promise(function (resolve) {
        setTimeout(function () {
          window.scrollTo(0, 0);
          resolve();
        }, 500);
      });
    },
    buttons: [
      {
        classes: "btn btn-success",
        text: "Next",
        action() {
          return this.next();
        },
      },
    ],
    title: "Welcome Back !",
    text: ["This is Step 1"],
  },

  {
    id: "intro1",
    attachTo: { element: "#register-tour", on: "bottom" },

    buttons: [
      {
        text: "Back",
        classes: "btn btn-light",
        action() {
          return this.back();
        },
      },
      {
        text: "Next",
        classes: "btn btn-success",
        action() {
          return this.next();
        },
      },
    ],
    title: "Register your account",
    text: "Get your Free Velzon account now.",
  },
  {
    id: "intro2",
    attachTo: { element: "#login-tour", on: "bottom" },
    buttons: [
      {
        text: "Back",
        classes: "btn btn-light",
        action() {
          return this.back();
        },
      },
      {
        text: "Next",
        classes: "btn btn-success",
        action() {
          return this.next();
        },
      },
    ],
    title: "Login your account",
    text: "Sign in to continue to Velzon.",
  },
  {
    id: "intro3",
    attachTo: { element: "#getproduct-tour", on: "bottom" },
    buttons: [
      {
        text: "Back",
        classes: "btn btn-light",
        action() {
          return this.back();
        },
      },
      {
        text: "Next",
        classes: "btn btn-success",
        action() {
          return this.next();
        },
      },
    ],
    title: "Get yout Product",
    text: "Sign in to continue to Velzon.",
  },
  {
    id: "intro4",
    attachTo: { element: "#thankyou-tour", on: "bottom" },
    buttons: [
      {
        text: "Back",
        classes: "btn btn-light",
        action() {
          return this.back();
        },
      },
      {
        text: "Thank you !",
        classes: "btn btn-primary",
        action() {
          return this.complete();
        },
      },
    ],
    title: "Thank you !",
    text: "Sign in to continue to Velzon.",
  },
];

export default steps;
