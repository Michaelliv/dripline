import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App } from "./app";

const container = document.getElementById("app");
if (!container) throw new Error("#app not found");

createRoot(container).render(
  React.createElement(BrowserRouter, null, React.createElement(App)),
);
