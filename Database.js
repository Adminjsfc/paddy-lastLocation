"use strict";
const fs = require('node:fs');
const mysql = require('mysql2/promise');
const pool = mysql.createPool({
  host: process.env.DB_HOST || '127.0.0.1',
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || 'sfc_cmr_app',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'sfc_cmr',
  timezone: '+05:30',
  connectionLimit: Number(process.env.DB_POOL_LIMIT || 10),
  waitForConnections: true,
  queueLimit: 100,
  connectTimeout: 10000,
  ...(process.env.DB_SSL_CA_FILE ? {ssl: {
    ca: fs.readFileSync(process.env.DB_SSL_CA_FILE), rejectUnauthorized: true
  }} : {})
});
pool.on('connection', connection => connection.query("SET time_zone='+05:30'"));
module.exports = {
  pool,
  async connect() {
    const connection = await pool.getConnection();
    try { await connection.ping(); } finally { connection.release(); }
  },
  async writeExecute(sql, args) {
    const [result] = await pool.execute(sql, args);
    return result;
  },
  close: () => pool.end()
};
