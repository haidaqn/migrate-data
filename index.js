const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
const { performance } = require('perf_hooks');
const { v7: uuidv7 } = require('uuid'); 

const mongoUrl = 'mongodb://localhost:27017';
const dbName = 'qlmt-test';
const collectionName = 'sensordatas';
const BATCH_SIZE = 1000; 

async function connectMySQL() {
  return await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Haidang2003x@',
    database: 'qlmt_test',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 60000,
    maxPreparedStatements: 100,
    maxPacketSize: 16777216
  });
}

async function setupDatabase(connection) {
  try {
    await connection.query(`CREATE DATABASE IF NOT EXISTS qlmt_test`);
    await connection.query(`USE qlmt_test`);
    
    await connection.query(`
      CREATE TABLE IF NOT EXISTS sensorsdata (
        _id VARCHAR(36) PRIMARY KEY,
        sensor VARCHAR(255),
        zone VARCHAR(255),
        value INT,
        timeReceived DATETIME NULL,
        minute INT,
        hour INT,
        day INT,
        month INT,
        year INT,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL,
        INDEX idx_sensor (sensor),
        INDEX idx_zone (zone),
        INDEX idx_time (year, month, day, hour, minute)
      )
    `);
    console.log('✅ Đã thiết lập database và table');
  } catch (error) {
    console.error('❌ Lỗi khi thiết lập database:', error);
    throw error;
  }
}

// Xử lý dữ liệu từ MongoDB
function transformMongoItem(item) {
  return {
    _id: uuidv7(),
    sensor: item.sensor || null,
    zone: item.zone || null,
    value: item.value || 0,
    timeReceived: item.timeReceived ? new Date(item.timeReceived.$date || item.timeReceived) : null,
    minute: item.minute || 0,
    hour: item.hour || 0,
    day: item.day || 0,
    month: item.month || 0,
    year: item.year || 0,
    createdAt: item.createdAt ? new Date(item.createdAt.$date || item.createdAt) : null,
    updatedAt: item.updatedAt ? new Date(item.updatedAt.$date || item.updatedAt) : null
  };
}

async function migrateData(connection) {
  const startTime = performance.now();
  let totalProcessed = 0;
  let totalBatches = 0;
  
  try {
    const client = new MongoClient(mongoUrl, { 
      useNewUrlParser: true, 
      useUnifiedTopology: true,
      maxPoolSize: 10
    });
    
    await client.connect();
    console.log('✅ Kết nối MongoDB thành công');
    
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    
    const totalDocuments = await collection.countDocuments();
    console.log(`🔍 Tổng số dòng cần migrate: ${totalDocuments}`);
    
    const cursor = collection.find({}).batchSize(BATCH_SIZE);
    let batch = [];
    
    while (await cursor.hasNext()) {
      const item = await cursor.next();
      const newItem = transformMongoItem(item);
      batch.push(newItem);
      
      // Insert theo batch khi đủ kích thước
      if (batch.length >= BATCH_SIZE) {
        await insertBatch(connection, batch);
        totalProcessed += batch.length;
        totalBatches++;
        
        const progress = (totalProcessed / totalDocuments * 100).toFixed(2);
        const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
        console.log(`⏱️ ${progress}% hoàn thành (${totalProcessed}/${totalDocuments}) - ${elapsedTime}s`);
        
        batch = [];
      }
    }
    
    if (batch.length > 0) {
      await insertBatch(connection, batch);
      totalProcessed += batch.length;
      totalBatches++;
    }
    
    const totalTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`🎉 Migration hoàn tất! Đã xử lý ${totalProcessed} dòng trong ${totalTime} giây (${totalBatches} batches)`);
    await client.close();
  } catch (error) {
    console.error('❌ Lỗi khi migrate dữ liệu:', error);
    throw error;
  }
}

async function insertBatch(connection, batch) {
  try {
    const placeholders = batch.map(() => '(?,?,?,?,?,?,?,?,?,?,?,?)').join(',');
    const flatValues = batch.flatMap(item => [
      item._id, 
      item.sensor, 
      item.zone, 
      item.value, 
      item.timeReceived, 
      item.minute, 
      item.hour, 
      item.day, 
      item.month, 
      item.year, 
      item.createdAt, 
      item.updatedAt
    ]);
    
    const query = `INSERT INTO sensorsdata (_id, sensor, zone, value, timeReceived, minute, hour, day, month, year, createdAt, updatedAt) 
                  VALUES ${placeholders} 
                  ON DUPLICATE KEY UPDATE 
                  sensor=VALUES(sensor), 
                  zone=VALUES(zone), 
                  value=VALUES(value)`;
                  
    await connection.query(query, flatValues);
  } catch (error) {
    console.error(`❌ Lỗi khi insert batch (${batch.length} dòng):`, error.message);
    
    // Nếu batch lớn bị lỗi, chia nhỏ và thử lại
    if (batch.length > 1) {
      console.log(`↪️ Chia nhỏ batch (${batch.length} dòng) và thử lại...`);
      const halfSize = Math.ceil(batch.length / 2);
      const firstHalf = batch.slice(0, halfSize);
      const secondHalf = batch.slice(halfSize);
      
      try {
        await insertBatch(connection, firstHalf);
        await insertBatch(connection, secondHalf);
        console.log(`✅ Đã xử lý thành công sau khi chia nhỏ`);
      } catch (subError) {
        console.error(`❌ Không thể xử lý sau khi chia nhỏ:`, subError.message);
        throw subError;
      }
    } else {
      console.error(`❌ Bỏ qua dòng có lỗi:`, JSON.stringify(batch[0]));
    }
  }
}

(async () => {
  let connection;
  try {
    console.log('🚀 Bắt đầu quá trình migrate...');
    connection = await connectMySQL();
    await setupDatabase(connection);
    await migrateData(connection);
  } catch (error) {
    console.error('❌ Lỗi chính:', error);
  } finally {
    if (connection) {
      await connection.end();
      console.log('👋 Đã đóng kết nối MySQL');
    }
  }
})();