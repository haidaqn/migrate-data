const { MongoClient } = require('mongodb');
const sql = require('mssql');
const { performance } = require('perf_hooks');
const { v7: uuidv7 } = require('uuid');

const mongoUrl = 'mongodb://localhost:27017';
const dbName = '';
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

const migrationOrder = [
  { collection: 'accounts', table: 'accounts' },
  { collection: 'companies', table: 'companies' },
  { collection: 'devices', table: 'devices' },
  { collection: 'sensors', table: 'sensors' },
  { collection: 'zones', table: 'zones' },
  { collection: 'sensor_zones', table: 'sensor_zones' },
  { collection: 'zlans', table: 'zlans' },
  { collection: 'memberscompany', table: 'memberscompany' },
  { collection: 'permissions', table: 'permissions' },
  { collection: 'warnings', table: 'warnings' },
  { collection: 'logs', table: 'logs' },
  { collection: 'mappings', table: 'mappings' },
  { collection: 'imagebinaries', table: 'imagebinaries' },
  { collection: 'cxviewdatas', table: 'cxviewdatas' },
  { collection: 'countings', table: 'countings' },
  { collection: 'sensordatas', table: 'sensordatas' }
];

const idMapping = {};

async function connectSQLServer() {
  try {
    console.log('🔄 Connecting to SQL Server...');
    
    const masterConfig = {
      server: sqlConfig.server,
      port: sqlConfig.port,
      user: sqlConfig.user,
      password: sqlConfig.password,
      database: 'master',
      options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
      }
    };
    
    await sql.connect(masterConfig);
    console.log('✅ Connected to SQL Server (master database)');
    
    return sql;
  } catch (err) {
    console.error('❌ SQL Server connection error:', err);
    throw err;
  }
}

async function setupDatabase() {
  try {
    await sql.query`
      IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'qlmt_test')
      BEGIN
        CREATE DATABASE qlmt_test;
      END
    `;
    
    await sql.query`USE qlmt_test`;
    
    await createTables();
    
    console.log('✅ Database and tables setup complete');
  } catch (error) {
    console.error('❌ Error setting up database:', error);
    throw error;
  }
}

async function createTables() {
  await sql.query`
    -- Accounts table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'accounts')
    BEGIN
      CREATE TABLE accounts (
        id VARCHAR(36) PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        name VARCHAR(255) NULL,
        password VARCHAR(255) NULL,
        phoneNumber VARCHAR(50) NULL,
        role VARCHAR(50) NULL,
        delete BIT DEFAULT 0,
        refreshToken VARCHAR(MAX) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Companies table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'companies')
    BEGIN
      CREATE TABLE companies (
        id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        account VARCHAR(36) NULL,
        logo VARCHAR(MAX) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Devices table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'devices')
    BEGIN
      CREATE TABLE devices (
        id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        company VARCHAR(36) NULL,
        type VARCHAR(50) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Sensors table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensors')
    BEGIN
      CREATE TABLE sensors (
        id VARCHAR(36) PRIMARY KEY,
        hexCode VARCHAR(255) NULL,
        sensorType VARCHAR(255) NULL,
        unit VARCHAR(50) NULL,
        factor VARCHAR(50) NULL,
        deviceId VARCHAR(255) NULL,
        device VARCHAR(36) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Zones table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'zones')
    BEGIN
      CREATE TABLE zones (
        id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        company VARCHAR(36) NULL,
        address VARCHAR(255) DEFAULT 'Viet Nam',
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- SensorZones table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensor_zones')
    BEGIN
      CREATE TABLE sensor_zones (
        id VARCHAR(36) PRIMARY KEY,
        sensor VARCHAR(36) NULL,
        zone VARCHAR(36) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- ZLans table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'zlans')
    BEGIN
      CREATE TABLE zlans (
        id VARCHAR(36) PRIMARY KEY,
        ip VARCHAR(50) NOT NULL,
        port VARCHAR(50) NULL,
        company VARCHAR(36) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- MembersCompany table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'memberscompany')
    BEGIN
      CREATE TABLE memberscompany (
        id VARCHAR(36) PRIMARY KEY,
        company VARCHAR(36) NULL,
        account VARCHAR(36) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Permissions table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'permissions')
    BEGIN
      CREATE TABLE permissions (
        id VARCHAR(36) PRIMARY KEY,
        zoneID VARCHAR(36) NULL,
        account VARCHAR(36) NULL,
        zone VARCHAR(50) NULL,
        sensor VARCHAR(50) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Warnings table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'warnings')
    BEGIN
      CREATE TABLE warnings (
        id VARCHAR(36) PRIMARY KEY,
        sensorType VARCHAR(255) NULL,
        value VARCHAR(255) NULL,
        color VARCHAR(50) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Logs table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'logs')
    BEGIN
      CREATE TABLE logs (
        id VARCHAR(36) PRIMARY KEY,
        type VARCHAR(255) NOT NULL,
        action VARCHAR(255) NOT NULL,
        description VARCHAR(MAX) NULL,
        status VARCHAR(50) NULL,
        dataOld VARCHAR(MAX) NULL,
        createBy VARCHAR(36) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Mappings table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mappings')
    BEGIN
      CREATE TABLE mappings (
        id VARCHAR(36) PRIMARY KEY,
        company VARCHAR(36) NULL,
        companyCode VARCHAR(255) NOT NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- ImageBinaries table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'imagebinaries')
    BEGIN
      CREATE TABLE imagebinaries (
        id VARCHAR(36) PRIMARY KEY,
        imageBinary TEXT NULL, 
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- CXViewData table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cxviewdatas')
    BEGIN
      CREATE TABLE cxviewdatas (
        id VARCHAR(36) PRIMARY KEY,
        idCXView VARCHAR(255) NULL,
        timestamp BIGINT NULL,
        image VARCHAR(36) NULL,
        groupID VARCHAR(255) NULL,
        groupName VARCHAR(255) NULL,
        isSatisfyingFace BIT NULL,
        cameraID VARCHAR(255) NULL,
        cameraName VARCHAR(255) NULL,
        name VARCHAR(255) NULL,
        gender VARCHAR(50) DEFAULT 'unknown',
        customerType VARCHAR(50) NULL,
        telephone VARCHAR(50) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- Countings table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'countings')
    BEGIN
      CREATE TABLE countings (
        id VARCHAR(36) PRIMARY KEY,
        timestamp BIGINT NULL,
        groupID VARCHAR(255) NULL,
        groupName VARCHAR(255) NULL,
        cameraID VARCHAR(255) NULL,
        cameraName VARCHAR(255) NULL,
        age VARCHAR(50) NULL,
        gender VARCHAR(50) NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
    END

    -- SensorData table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensordatas')
    BEGIN
      CREATE TABLE sensordatas (
        id VARCHAR(36) PRIMARY KEY,
        sensor VARCHAR(255) NULL,
        zone VARCHAR(255) NULL,
        value INT NULL,
        timeReceived DATETIME NULL,
        minute INT NULL,
        hour INT NULL,
        day INT NULL,
        month INT NULL,
        year INT NULL,
        createdAt DATETIME NULL,
        updatedAt DATETIME NULL
      );
      
      CREATE INDEX idx_sensor ON sensordatas(sensor);
      CREATE INDEX idx_zone ON sensordatas(zone);
      CREATE INDEX idx_time ON sensordatas(year, month, day, hour, minute);
    END
  `;
}

function transformObjectId(item) {
  if (item._id) {
    if (item._id.$oid) {
      return item._id.$oid;
    } else if (typeof item._id === 'object' && item._id.toString) {
      return item._id.toString();
    }
    return item._id;
  }
  return uuidv7(); 
}

function transformDate(date) {
  if (!date) return null;
  
  if (date.$date) {
    return new Date(date.$date);
  } else if (typeof date === 'string') {
    return new Date(date);
  } else if (date instanceof Date) {
    return date;
  }
  return null;
}

function transformDocument(item, collectionName) {
  const originalId = transformObjectId(item);
  const newId = uuidv7();
  idMapping[`${collectionName}:${originalId}`] = newId;
  
  const transformed = {
    id: newId,
    createdAt: transformDate(item.createdAt),
    updatedAt: transformDate(item.updatedAt)
  };
  
  switch (collectionName) {
    case 'accounts':
      return {
        ...transformed,
        email: item.email || '',
        name: item.name || '',
        password: item.password || '',
        phoneNumber: item.phoneNumber || '',
        role: item.role || 'USER',
        delete: item.delete || false,
        refreshToken: item.refreshToken || null
      };
      
    case 'companies':
      return {
        ...transformed,
        name: item.name || '',
        account: item.account ? idMapping[`accounts:${transformObjectId(item.account)}`] || null : null,
        logo: item.logo || ''
      };
      
    case 'devices':
      return {
        ...transformed,
        name: item.name || '',
        company: item.company ? idMapping[`companies:${transformObjectId(item.company)}`] || null : null,
        type: item.type || 'SENSOR'
      };
      
    case 'sensors':
      return {
        ...transformed,
        hexCode: item.hexCode || '',
        sensorType: item.sensorType || '',
        unit: item.unit || '',
        factor: item.factor || '',
        deviceId: item.deviceId || '',
        device: item.device ? idMapping[`devices:${transformObjectId(item.device)}`] || null : null
      };
      
    case 'zones':
      return {
        ...transformed,
        name: item.name || '',
        company: item.company ? idMapping[`companies:${transformObjectId(item.company)}`] || null : null,
        address: item.address || 'Viet Nam'
      };
      
    case 'sensor_zones':
      return {
        ...transformed,
        sensor: item.sensor ? idMapping[`sensors:${transformObjectId(item.sensor)}`] || null : null,
        zone: item.zone ? idMapping[`zones:${transformObjectId(item.zone)}`] || null : null
      };
      
    case 'zlans':
      return {
        ...transformed,
        ip: item.ip || '',
        port: item.port || '',
        company: item.company ? idMapping[`companies:${transformObjectId(item.company)}`] || null : null
      };
      
    case 'memberscompany':
      return {
        ...transformed,
        company: item.company ? idMapping[`companies:${transformObjectId(item.company)}`] || null : null,
        account: item.account ? idMapping[`accounts:${transformObjectId(item.account)}`] || null : null
      };
      
    case 'permissions':
      return {
        ...transformed,
        zoneID: item.zoneID ? idMapping[`zones:${transformObjectId(item.zoneID)}`] || null : null,
        account: item.account ? idMapping[`accounts:${transformObjectId(item.account)}`] || null : null,
        zone: item.zone || 'NULL',
        sensor: item.sensor || 'NULL'
      };
      
    case 'warnings':
      return {
        ...transformed,
        sensorType: item.sensorType || '',
        value: item.value || '',
        color: item.color || 'DEFAULT'
      };
      
    case 'logs':
      return {
        ...transformed,
        type: item.type || '',
        action: item.action || '',
        description: item.description || '',
        status: item.status || '',
        dataOld: item.dataOld || '',
        createBy: item.createBy ? idMapping[`accounts:${transformObjectId(item.createBy)}`] || null : null
      };
      
    case 'mappings':
      return {
        ...transformed,
        company: item.company ? idMapping[`companies:${transformObjectId(item.company)}`] || null : null,
        companyCode: item.companyCode || ''
      };
      
    case 'imagebinaries':
      return {
        ...transformed,
        imageBinary: item.imageBinary || null
      };
      
    case 'cxviewdatas':
      return {
        ...transformed,
        idCXView: item.idCXView || '',
        timestamp: item.timestamp || 0,
        image: item.image ? idMapping[`imagebinaries:${transformObjectId(item.image)}`] || null : null,
        groupID: item.groupID || '',
        groupName: item.groupName || '',
        isSatisfyingFace: item.isSatisfyingFace || false,
        cameraID: item.cameraID || '',
        cameraName: item.cameraName || '',
        name: item.name || '',
        gender: item.gender || 'unknown',
        customerType: item.customerType || '',
        telephone: item.telephone || ''
      };
      
    case 'countings':
      return {
        ...transformed,
        timestamp: item.timestamp || 0,
        groupID: item.groupID || '',
        groupName: item.groupName || '',
        cameraID: item.cameraID || '',
        cameraName: item.cameraName || '',
        age: item.age || '',
        gender: item.gender || ''
      };
      
    case 'sensordatas':
      return {
        ...transformed,
        sensor: item.sensor || '',
        zone: item.zone || '',
        value: item.value || 0,
        timeReceived: transformDate(item.timeReceived),
        minute: item.minute || 0,
        hour: item.hour || 0,
        day: item.day || 0,
        month: item.month || 0,
        year: item.year || 0
      };
      
    default:
      return transformed;
  }
}

async function migrateCollection(collection, table) {
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
    console.log(`✅ Connected to MongoDB for ${collection}`);
    
    const db = mongoClient.db(dbName);
    const mongoCollection = db.collection(collection);
    
    const totalDocuments = await mongoCollection.countDocuments();
    console.log(`🔍 Total documents to migrate in ${collection}: ${totalDocuments}`);
    
    if (totalDocuments === 0) {
      console.log(`⚠️ No documents found in ${collection}, skipping...`);
      await mongoClient.close();
      return;
    }
    
    const cursor = mongoCollection.find({}).batchSize(BATCH_SIZE);
    let batch = [];
    
    while (await cursor.hasNext()) {
      const item = await cursor.next();
      const transformedItem = transformDocument(item, collection);
      batch.push(transformedItem);
      
      if (batch.length >= BATCH_SIZE) {
        await insertBatch(batch, table);
        totalProcessed += batch.length;
        totalBatches++;
        
        const progress = (totalProcessed / totalDocuments * 100).toFixed(2);
        const elapsedTime = ((performance.now() - startTime) / 1000).toFixed(2);
        console.log(`⏱️ ${progress}% complete (${totalProcessed}/${totalDocuments}) - ${elapsedTime}s`);
        
        batch = [];
      }
    }
    
    if (batch.length > 0) {
      await insertBatch(batch, table);
      totalProcessed += batch.length;
      totalBatches++;
    }
    
    const totalTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`🎉 Migration of ${collection} complete! Processed ${totalProcessed} documents in ${totalTime} seconds (${totalBatches} batches)`);
    await mongoClient.close();
  } catch (error) {
    console.error(`❌ Error migrating ${collection}:`, error);
    throw error;
  }
}

async function insertBatch(batch, tableName) {
  try {
    const transaction = new sql.Transaction();
    await transaction.begin();

    const request = new sql.Request(transaction);
    for (const item of batch) {
      const columns = Object.keys(item).map((col) => `[${col}]`).join(', ');
      const values = Object.keys(item)
        .map((col) => `@${col}`)
        .join(', ');

      const query = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
      for (const [key, value] of Object.entries(item)) {
        request.input(key, value);
      }
      await request.query(query);
    }

    await transaction.commit();
    console.log(`✅ Batch inserted into ${tableName}`);
  } catch (error) {
    console.error(`❌ Error inserting batch into ${tableName}:`, error);
    throw error;
  }
}

async function migrateAllCollections() {
  for (const { collection, table } of migrationOrder) {
    console.log(`🔄 Starting migration for collection: ${collection} -> table: ${table}`);
    try {
      await migrateCollection(collection, table);
    } catch (error) {
      console.error(`❌ Migration failed for ${collection}:`, error);
    }
  }
  console.log('🎉 All collections migrated successfully!');
}

async function main() {
  try {
    await connectSQLServer();
    await setupDatabase();
    await migrateAllCollections();
  } catch (error) {
    console.error('❌ Migration process failed:', error);
  } finally {
    sql.close();
  }
}

main();