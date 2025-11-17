const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors()); // Allow requests from React frontend
app.use(express.json()); // Parse JSON request bodies

// PostgreSQL connection pool
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Database connection error:', err);
  } else {
    console.log('Database connected successfully');
  }
});

// API Routes

// Get available tables/datasets
app.get('/api/tables', async (req, res) => {
  try {
    const query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      ORDER BY table_name;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching tables:', err);
    res.status(500).json({ error: 'Failed to fetch tables' });
  }
});

// Get columns for a specific table
app.get('/api/tables/:tableName/columns', async (req, res) => {
  try {
    const { tableName } = req.params;
    const query = `
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = $1 AND table_schema = 'public'
      ORDER BY ordinal_position;
    `;
    const result = await pool.query(query, [tableName]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching columns:', err);
    res.status(500).json({ error: 'Failed to fetch columns' });
  }
});

// Get data from a specific table with optional filtering
app.get('/api/data/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const { 
      limit = 100, 
      offset = 0,
      booking_status,
      market_segment,
      market_segment_text,
      min_price,
      max_price,
      sort_by,
      sort_order = 'ASC'
    } = req.query;

    // Build WHERE clause dynamically based on filters
    let whereConditions = [];
    let queryParams = [];
    let paramIndex = 1;

    if (booking_status) {
      whereConditions.push(`booking_status = $${paramIndex}`);
      queryParams.push(booking_status);
      paramIndex++;
    }

    if (market_segment) {
      whereConditions.push(`market_segment = $${paramIndex}`);
      queryParams.push(market_segment);
      paramIndex++;
    }

    if (min_price) {
      whereConditions.push(`avg_price_per_room >= $${paramIndex}`);
      queryParams.push(parseFloat(min_price));
      paramIndex++;
    }

    if (max_price) {
      whereConditions.push(`avg_price_per_room <= $${paramIndex}`);
      queryParams.push(parseFloat(max_price));
      paramIndex++;
    }

    const whereClause = whereConditions.length > 0 
      ? 'WHERE ' + whereConditions.join(' AND ') 
      : '';

    // Build ORDER BY clause
    const orderByClause = sort_by 
      ? `ORDER BY ${sort_by} ${sort_order}` 
      : '';

    // Add limit and offset to params
    queryParams.push(parseInt(limit));
    queryParams.push(parseInt(offset));

    // Main query
    const query = `
      SELECT * FROM ${tableName}
      ${whereClause}
      ${orderByClause}
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;

    // Count query for pagination
    const countQuery = `
      SELECT COUNT(*) FROM ${tableName}
      ${whereClause}
    `;

    const [dataResult, countResult] = await Promise.all([
      pool.query(query, queryParams),
      pool.query(countQuery, queryParams.slice(0, -2)) // Exclude limit/offset from count
    ]);

    res.json({
      data: dataResult.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });
  } catch (err) {
    console.error('Error fetching data:', err);
    res.status(500).json({ error: 'Failed to fetch data', details: err.message });
  }
});

// Get distinct values for a column (useful for filter dropdowns)
app.get('/api/data/:tableName/distinct/:columnName', async (req, res) => {
  try {
    const { tableName, columnName } = req.params;
    const query = `
      SELECT DISTINCT ${columnName} 
      FROM ${tableName} 
      WHERE ${columnName} IS NOT NULL
      ORDER BY ${columnName}
      LIMIT 100;
    `;
    const result = await pool.query(query);
    res.json(result.rows.map(row => row[columnName]));
  } catch (err) {
    console.error('Error fetching distinct values:', err);
    res.status(500).json({ error: 'Failed to fetch distinct values' });
  }
});

// Get summary statistics
app.get('/api/stats/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const query = `
      SELECT 
        COUNT(*) as total_records,
        AVG(avg_price_per_room) as avg_price,
        MIN(avg_price_per_room) as min_price,
        MAX(avg_price_per_room) as max_price
      FROM ${tableName}
      WHERE avg_price_per_room IS NOT NULL;
    `;
    const result = await pool.query(query);
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Error fetching stats:', err);
    res.status(500).json({ error: 'Failed to fetch statistics' });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  pool.end();
  process.exit(0);
});