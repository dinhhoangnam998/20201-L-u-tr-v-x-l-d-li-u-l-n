import React from "react";
import { Navigate } from "react-router-dom";
import DashboardLayout from "src/layouts/DashboardLayout";
import MainLayout from "src/layouts/MainLayout";
import NotFoundView from "src/utils/NotFoundView";

const routes = [
  {
    path: "/app",
    element: <DashboardLayout />,
    children: [
      // { path: "account", element: <AccountView /> },
      { path: "*", element: <Navigate to="/404" /> },
    ],
  },
  {
    path: "/",
    element: <MainLayout />,
    children: [
      { path: "404", element: <NotFoundView /> },
      { path: "/", element: <Navigate to="/app" /> },
      { path: "*", element: <Navigate to="/404" /> },
    ],
  },
];

export default routes;
