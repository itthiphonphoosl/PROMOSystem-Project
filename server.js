require("dotenv").config({ quiet: true });
const express = require("express");
const cors = require("cors");
const os = require("os");

// Routes
const authRoutes = require("./routes/auth.routes");
const userRoutes = require("./routes/user.routes");
const stationRoutes = require("./routes/station.routes");
const machineRoutes = require("./routes/machine.routes");

// DB
const { getPool } = require("./config/db");

const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => res.json({ ok: true, project: "PROMOS" }));

// Auth (login/logout + whatever you already have under /api/auth)
app.use("/api/auth", authRoutes);

// Users (current user + user list + user by id)
app.use("/api", userRoutes);

app.use("/api", stationRoutes);
app.use("/api", machineRoutes);

function getLanIp() {
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === "IPv4" && !net.internal) return net.address;
    }
  }
  return "localhost";
}

const port = Number(process.env.PORT || 4030);
const host = "0.0.0.0";

// Trigger DB connect at startup
getPool();

app.listen(port, host, () => {
  console.log(`🚀 Server listening on port ${port}`);
  console.log(`🌐 LAN URL: http://${getLanIp()}:${port}`);
});
