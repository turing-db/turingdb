import { render } from "preact";

import "./style.css";
import "normalize.css";

import { TAppBar } from "@components/appbar"
import { TMainFrame } from "@components/mainframe"
import { TFrame } from "@components/frame"

export function App() {
  return (
    <TMainFrame>
      <TAppBar/>
      <TFrame/>
    </TMainFrame>
  );
}

render(<App />, document.getElementById("app"));
