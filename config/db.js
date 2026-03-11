const mysql = require("mysql2/promise");

let pool;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host:               process.env.DB_HOST,
      port:               Number(process.env.DB_PORT || 3306),
      user:               process.env.DB_USER,
      password:           process.env.DB_PASSWORD,
      database:           process.env.DB_DATABASE,
      waitForConnections: true,
      connectionLimit:    10,
      charset:            "utf8mb4",
    });

    // ทดสอบ connect ตอนเริ่ม server
    pool.getConnection()
      .then((conn) => {
        console.log("✅ DB connected (MySQL)");
        conn.release();
      })
      .catch((err) => {
        console.log("❌ DB connection failed:", err.message);
      });
  }
  return pool;
}

module.exports = { getPool };