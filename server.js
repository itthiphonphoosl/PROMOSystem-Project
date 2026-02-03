// server.js
require("dotenv").config({ quiet: true });
const express = require("express");
const cors = require("cors");
const os = require("os");

const authRoutes = require("./routes/auth.routes");
const trayRoutes = require("./routes/tray.routes");
const opScanRoutes = require("./routes/opScan.routes");
const masterRoutes = require("./routes/master.routes");

const { requireAuth, requireRole } = require("./middleware/auth.middleware");
const { getPool } = require("./config/db");

const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => res.json({ ok: true, project: "PROMOS" }));

app.use("/api/auth", authRoutes);

app.use("/api/master", masterRoutes);
app.use("/api/trays", trayRoutes);
app.use("/api/op-scans", opScanRoutes);

app.get("/api/me", requireAuth, (req, res) => {
  res.json({ user: req.user });
});

app.get("/api/supervisor", requireAuth, requireRole(["admin", "manager"]), (req, res) => {
  res.json({ message: "Supervisor access granted", user: req.user });
});

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

// trigger DB connection at startup
getPool();

app.listen(port, host, () => {
  console.log(`🚀 Server listening on port ${port}`);
  console.log(`🌐 LAN URL: http://${getLanIp()}:${port}`);
});
