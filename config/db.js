const sql = require("mssql");

const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  port: Number(process.env.DB_PORT || 1433),
  options: {
    enableArithAbort: String(process.env.DB_ENABLE_ARITH_ABORT).toLowerCase() === "true",
    encrypt: String(process.env.DB_ENCRYPT).toLowerCase() === "true",
    trustServerCertificate: true,
  },
};

let poolPromise;

function getPool() {
  if (!poolPromise) {
    poolPromise = new sql.ConnectionPool(config)
      .connect()
      .then((pool) => {
        console.log("✅ DB connected");
        return pool;
      })
      .catch((err) => {
        console.log("❌ DB connection failed:", err.message);
        return null;
      });
  }
  return poolPromise;
}

module.exports = { sql, getPool };
