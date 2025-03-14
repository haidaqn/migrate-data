const { MongoClient } = require('mongodb');
const sql = require('mssql');
const { performance } = require('perf_hooks');
const { v7: uuidv7 } = require('uuid');

const mongoUrl = 'mongodb://localhost:27017';
const dbName = 'ids_sensor';
const collectionName = 'sensordatas';
const BATCH_SIZE = 1000;

const sqlConfig = {
    server: 'localhost',
    port: 1433,
    database: 'qlmt_test', 
    user: 'sa',
    password: 'StrongPassword123!', 
    options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
    }
};


async function connectSQLServer() {
    try {
      console.log('🔄 Thử kết nối với SQL Server...');
      
      // Thử kết nối đến master database trước
      const masterConfig = {
        server: sqlConfig.server,
        port: sqlConfig.port,
        user: sqlConfig.user,
        password: sqlConfig.password,
        database: 'master', // Kết nối đến master database trước
        options: {
          encrypt: false,
          trustServerCertificate: true,
          enableArithAbort: true
        }
      };
      
      await sql.connect(masterConfig);
      console.log('✅ Kết nối đến SQL Server thành công (master database)');
      
      return sql;
    } catch (err) {
      console.error('❌ Lỗi kết nối SQL Server:', err);
      throw err;
    }
  }


async function setupDatabase() {
  try {
    // Tạo database nếu chưa tồn tại
    await sql.query`
      IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'qlmt_test')
      BEGIN
        CREATE DATABASE qlmt_test;
      END
    `;
    
    // Chuyển sang sử dụng database qlmt_test
    await sql.query`USE qlmt_test`;
    
    // Tạo bảng nếu chưa tồn tại
    await sql.query`
      IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensorsdata')
      BEGIN
        CREATE TABLE sensorsdata (
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
          updatedAt DATETIME NULL
        );
        
        CREATE INDEX idx_sensor ON sensorsdata(sensor);
        CREATE INDEX idx_zone ON sensorsdata(zone);
        CREATE INDEX idx_time ON sensorsdata(year, month, day, hour, minute);
      END
    `;
    
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

async function migrateData() {
  const startTime = performance.now();
  let totalProcessed = 0;
  let totalBatches = 0;
  
  try {
    const mongoClient = new MongoClient(mongoUrl, { 
      useNewUrlParser: true, 
      useUnifiedTopology: true,
      maxPoolSize: 10
    });
    
    await mongoClient.connect();
    console.log('✅ Kết nối MongoDB thành công');
    
    const db = mongoClient.db(dbName);
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
        await insertBatch(batch);
        totalProcessed += batch.length;
        totalBatches++;
        
        const progress = (totalProcessed / totalDocuments * 100).toFixed(2);
        const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
        console.log(`⏱️ ${progress}% hoàn thành (${totalProcessed}/${totalDocuments}) - ${elapsedTime}s`);
        
        batch = [];
      }
    }
    
    if (batch.length > 0) {
      await insertBatch(batch);
      totalProcessed += batch.length;
      totalBatches++;
    }
    
    const totalTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`🎉 Migration hoàn tất! Đã xử lý ${totalProcessed} dòng trong ${totalTime} giây (${totalBatches} batches)`);
    await mongoClient.close();
  } catch (error) {
    console.error('❌ Lỗi khi migrate dữ liệu:', error);
    throw error;
  }
}

async function insertBatch(batch) {
  try {
    // Sử dụng đối tượng Transaction để đảm bảo tính toàn vẹn
    const transaction = new sql.Transaction();
    
    await new Promise((resolve, reject) => {
      transaction.begin(err => {
        if (err) {
          reject(err);
          return;
        }
        
        // Tạo request mới trong transaction
        const request = new sql.Request(transaction);
        
        // Tạo table variable
        const table = new sql.Table('sensorsdata');
        table.create = false;
        table.columns.add('_id', sql.VarChar(36), { nullable: false });
        table.columns.add('sensor', sql.VarChar(255), { nullable: true });
        table.columns.add('zone', sql.VarChar(255), { nullable: true });
        table.columns.add('value', sql.Int, { nullable: true });
        table.columns.add('timeReceived', sql.DateTime, { nullable: true });
        table.columns.add('minute', sql.Int, { nullable: true });
        table.columns.add('hour', sql.Int, { nullable: true });
        table.columns.add('day', sql.Int, { nullable: true });
        table.columns.add('month', sql.Int, { nullable: true });
        table.columns.add('year', sql.Int, { nullable: true });
        table.columns.add('createdAt', sql.DateTime, { nullable: true });
        table.columns.add('updatedAt', sql.DateTime, { nullable: true });
        
        // Thêm dữ liệu vào bảng tạm
        batch.forEach(item => {
          table.rows.add(
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
          );
        });
        
        // Thực hiện bulk insert
        request.bulk(table, (err, result) => {
          if (err) {
            transaction.rollback(rollbackErr => {
              reject(err); // Reject với lỗi gốc
            });
          } else {
            transaction.commit(commitErr => {
              if (commitErr) {
                reject(commitErr);
              } else {
                resolve(result);
              }
            });
          }
        });
      });
    });
    
  } catch (error) {
    console.error(`❌ Lỗi khi insert batch (${batch.length} dòng):`, error.message);
    
    // Nếu batch lớn bị lỗi, chia nhỏ và thử lại
    if (batch.length > 1) {
      console.log(`↪️ Chia nhỏ batch (${batch.length} dòng) và thử lại...`);
      const halfSize = Math.ceil(batch.length / 2);
      const firstHalf = batch.slice(0, halfSize);
      const secondHalf = batch.slice(halfSize);
      
      try {
        await insertBatch(firstHalf);
        await insertBatch(secondHalf);
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

// Thay thế hàm setupMergeOperation để hỗ trợ tốt hơn với SQL Server cơ bản
async function setupMergeOperation() {
  try {
    // Kiểm tra version của SQL Server và điều chỉnh tính năng hỗ trợ
    const versionResult = await sql.query`SELECT @@VERSION as version`;
    const sqlVersion = versionResult.recordset[0].version;
    console.log(`📊 SQL Server Version: ${sqlVersion}`);
    
    // Nếu SQL Server hỗ trợ MERGE statement (SQL Server 2008+)
    if (sqlVersion.includes('SQL Server')) {
      // Tạo stored procedure để xử lý upsert
      await sql.query`
        IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'UpsertSensorData')
        BEGIN
          EXEC('
            CREATE PROCEDURE UpsertSensorData
              @id VARCHAR(36),
              @sensor VARCHAR(255),
              @zone VARCHAR(255), 
              @value INT,
              @timeReceived DATETIME,
              @minute INT,
              @hour INT,
              @day INT,
              @month INT,
              @year INT,
              @createdAt DATETIME,
              @updatedAt DATETIME
            AS
            BEGIN
              SET NOCOUNT ON;
              
              IF EXISTS (SELECT 1 FROM sensorsdata WHERE _id = @id)
              BEGIN
                UPDATE sensorsdata SET
                  sensor = @sensor,
                  zone = @zone,
                  value = @value
                WHERE _id = @id
              END
              ELSE
              BEGIN
                INSERT INTO sensorsdata (_id, sensor, zone, value, timeReceived, minute, hour, day, month, year, createdAt, updatedAt)
                VALUES (@id, @sensor, @zone, @value, @timeReceived, @minute, @hour, @day, @month, @year, @createdAt, @updatedAt)
              END
            END
          ')
        END
      `;
      
      console.log('✅ Đã tạo stored procedure cho upsert');
    } else {
      console.log('⚠️ SQL Server có thể không hỗ trợ đầy đủ MERGE, sẽ sử dụng insert trực tiếp');
    }
  } catch (error) {
    console.error('⚠️ Lỗi khi tạo stored procedure, tiếp tục với insert trực tiếp:', error);
    // Không throw error để tiếp tục quá trình
  }
}

(async () => {
  try {
    console.log('🚀 Bắt đầu quá trình migrate...');
    await connectSQLServer();
    await setupDatabase();
    await setupMergeOperation();
    await migrateData();
  } catch (error) {
    console.error('❌ Lỗi chính:', error);
  } finally {
    if (sql.connected) {
      await sql.close();
      console.log('👋 Đã đóng kết nối SQL Server');
    }
  }
})();