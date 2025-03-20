const { MongoClient } = require('mongodb');
const sql = require('mssql');
const { performance } = require('perf_hooks');
const { v7: uuidv7 } = require('uuid');




const migrationOrder = [
  { collection: 'accounts', table: 'accounts' },
  { collection: 'logs', table: 'logs' },
  { collection: 'companies', table: 'companies' },
  { collection: 'zlans', table: 'zlans' },
  { collection: 'warnings', table: 'warnings' },
  { collection: 'membercompanies', table: 'membercompanies' },
  { collection: 'zones', table: 'zones' },
  { collection: 'mappings', table: 'mappings' },
  { collection: 'devices', table: 'devices' },
  { collection: 'sensors', table: 'sensors' },
  { collection: 'sensorzones', table: 'sensorzones' },
  { collection: 'permissions', table: 'permissions' },
  { collection: 'cxviewdatas', table: 'cxviewdatas' },
  { collection: 'imagebinaries', table: 'imagebinaries' },

  { collection: 'countings', table: 'countings' },
];

const idMapping = {};

async function connectSQLServer() {
  try {
    console.log('🔄 Connecting to SQL Server...');
    await sql.connect(sqlConfig);
    console.log('✅ Connected to SQL Server');
    return sql;
  } catch (err) {
    console.error('❌ SQL Server connection error:', err);
    throw err;
  }
}

async function setupDatabase() {
  try {
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
        [delete] BIT DEFAULT 0,
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
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensorzones')
    BEGIN
      CREATE TABLE sensorzones (
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

    -- membercompanies table
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'membercompanies')
    BEGIN
      CREATE TABLE membercompanies (
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

  
  `;
}

function transformDate(date) {
  if (!date) return null;
  if (date.$date) return new Date(date.$date);
  if (typeof date === 'string' || date instanceof Date) return new Date(date);
  return null;
}

function transformDocument(item, collectionName) {
  const newId = uuidv7();
  const originalId = item._id ? (item._id.$oid || item._id.toString()) : uuidv7();
  idMapping[`${collectionName}:${originalId}`] = newId;

  const transformed = {
    id: newId,
    createdAt: transformDate(item.createdAt),
    updatedAt: transformDate(item.updatedAt),
  };

  switch (collectionName) {
    case 'accounts':
      return {
        ...transformed,
        email: item.email || '',
        name: item.name || null,
        password: item.password || null,
        phoneNumber: item.phoneNumber || null,
        role: item.role || 'USER',
        delete: item.delete || false,
        refreshToken: item.refreshToken || null,
      };
    case 'companies':
      return {
        ...transformed,
        name: item.name || '',
        account: item.account ? idMapping[`accounts:${item.account.$oid || item.account.toString()}`] || uuidv7() : null,
        logo: item.logo || null,
      };
    case 'devices':
      return {
        ...transformed,
        name: item.name || '',
        company: item.company ? idMapping[`companies:${item.company.$oid || item.company.toString()}`] || uuidv7() : null,
        type: item.type || 'SENSOR',
      };
    case 'sensors':
      return {
        ...transformed,
        hexCode: item.hexCode || null,
        sensorType: item.sensorType || null,
        unit: item.unit || null,
        factor: item.factor || null,
        deviceId: item.deviceId || null,
        device: item.device ? idMapping[`devices:${item.device.$oid || item.device.toString()}`] || uuidv7() : null,
      };
    case 'zones':
      return {
        ...transformed,
        name: item.name || '',
        company: item.company ? idMapping[`companies:${item.company.$oid || item.company.toString()}`] || uuidv7() : null,
        address: item.address || 'Viet Nam',
      };
    case 'sensorzones':
      return {
        ...transformed,
        sensor: item.sensor ? idMapping[`sensors:${item.sensor.$oid || item.sensor.toString()}`] || uuidv7() : null,
        zone: item.zone ? idMapping[`zones:${item.zone.$oid || item.zone.toString()}`] || uuidv7() : null,
      };
    case 'zlans':
      return {
        ...transformed,
        ip: item.ip || '',
        port: item.port || null,
        company: item.company ? idMapping[`companies:${item.company.$oid || item.company.toString()}`] || uuidv7() : null,
      };
    case 'membercompanies':
      return {
        ...transformed,
        company: item.company ? idMapping[`companies:${item.company.$oid || item.company.toString()}`] || uuidv7() : null,
        account: item.account ? idMapping[`accounts:${item.account.$oid || item.account.toString()}`] || uuidv7() : null,
      };
    case 'permissions':
      return {
        ...transformed,
        zoneID: item.zoneID ? idMapping[`zones:${item.zoneID.$oid || item.zoneID.toString()}`] || uuidv7() : null,
        account: item.account ? idMapping[`accounts:${item.account.$oid || item.account.toString()}`] || uuidv7() : null,
        zone: item.zone || 'NULL',
        sensor: item.sensor || 'NULL',
      };
    case 'warnings':
      return {
        ...transformed,
        sensorType: item.sensorType || null,
        value: item.value || null,
        color: item.color || 'DEFAULT',
      };
    case 'logs':
      return {
        ...transformed,
        type: item.type || '',
        action: item.action || '',
        description: item.description || null,
        status: item.status || null,
        dataOld: item.dataOld || null,
        createBy: item.createBy ? idMapping[`accounts:${item.createBy.$oid || item.createBy.toString()}`] || uuidv7() : null,
      };
    case 'mappings':
      return {
        ...transformed,
        company: item.company ? idMapping[`companies:${item.company.$oid || item.company.toString()}`] || uuidv7() : null,
        companyCode: item.companyCode || '',
      };
    case 'imagebinaries':
      return {
        ...transformed,
        imageBinary: item.imageBinary || null,
      };
    case 'cxviewdatas':
      return {
        ...transformed,
        idCXView: item.idCXView || null,
        timestamp: item.timestamp || null,
        image: item.image ? idMapping[`imagebinaries:${item.image.$oid || item.image.toString()}`] || uuidv7() : null,
        groupID: item.groupID || null,
        groupName: item.groupName || null,
        isSatisfyingFace: item.isSatisfyingFace || null,
        cameraID: item.cameraID || null,
        cameraName: item.cameraName || null,
        name: item.name || null,
        gender: item.gender || 'unknown',
        customerType: item.customerType || null,
        telephone: item.telephone || null,
      };
    case 'countings':
      return {
        ...transformed,
        timestamp: item.timestamp || null,
        groupID: item.groupID || null,
        groupName: item.groupName || null,
        cameraID: item.cameraID || null,
        cameraName: item.cameraName || null,
        age: item.age || null,
        gender: item.gender || null,
      };
    
    default:
      return transformed;
  }
}

async function migrateCollection(collection, table) {
  const startTime = performance.now();
  let totalProcessed = 0;

  try {
    const mongoClient = new MongoClient(mongoUrl);
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

    const cursor = mongoCollection.find({}).batchSize(1000);
    let batch = [];

    while (await cursor.hasNext()) {
      const item = await cursor.next();
      const transformedItem = transformDocument(item, collection);
      batch.push(transformedItem);

      if (batch.length >= 1000) {
        await insertBatch(batch, table);
        totalProcessed += batch.length;
        console.log(`⏱️ Processed ${totalProcessed}/${totalDocuments} in ${collection}`);
        batch = [];
      }
    }

    if (batch.length > 0) {
      await insertBatch(batch, table);
      totalProcessed += batch.length;
    }

    const totalTime = ((performance.now() - startTime) / 1000).toFixed(2);
    console.log(`🎉 Migration of ${collection} complete! Processed ${totalProcessed} documents in ${totalTime}s`);
    await mongoClient.close();
  } catch (error) {
    console.error(`❌ Error migrating ${collection}:`, error);
    throw error;
  }
}

async function insertBatch(batch, tableName) {
  const pool = await sql.connect(sqlConfig);
  const transaction = new sql.Transaction(pool);
  try {
    await transaction.begin();
    const request = new sql.Request(transaction);

    for (const item of batch) {
      const columns = Object.keys(item).map(col => `[${col}]`).join(', ');
      const params = Object.keys(item).map((_, i) => `@p${i}`).join(', ');
      const query = `INSERT INTO ${tableName} (${columns}) VALUES (${params})`;

      Object.keys(item).forEach((col, i) => {
        request.input(`p${i}`, item[col]);
      });

      await request.query(query);
    }

    await transaction.commit();
    console.log(`✅ Inserted batch into ${tableName} (${batch.length} records)`);
  } catch (error) {
    await transaction.rollback();
    console.error(`❌ Error inserting batch into ${tableName}:`, error);
    throw error;
  } finally {
    pool.close();
  }
}

async function migrateAllCollections() {
  for (const { collection, table } of migrationOrder) {
    console.log(`🔄 Starting migration: ${collection} -> ${table}`);
    await migrateCollection(collection, table);
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
    await sql.close();
  }
}

main();