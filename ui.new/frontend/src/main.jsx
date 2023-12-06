// React
import ReactDOM from "react-dom/client";
import "./index.css";
import "./index.scss";

// Blueprintjs dependencies
//import "normalize.css/normalize.css";
//import "@fontsource/public-sans";
import "@blueprintjs/icons/lib/css/blueprint-icons.css";
import "@blueprintjs/core/lib/css/blueprint.css";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import AppController from "./App";

const queryClient = new QueryClient();
const root = ReactDOM.createRoot(document.getElementById("root"));

root.render(
  <QueryClientProvider client={queryClient}>
    <AppController/>
  </QueryClientProvider>
);
