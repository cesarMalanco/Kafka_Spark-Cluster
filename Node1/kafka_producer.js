const { Kafka } = require('kafkajs');
const fs = require('fs');
const { faker } = require('@faker-js/faker/locale/es');

const kafka = new Kafka({
  clientId: 'productor',
  brokers: ['192.168.1.101:9092','192.168.1.102:9092','192.168.1.103:9092'],
  connectionTimeout: 30000,
  requestTimeout: 30000,
});

const producer = kafka.producer();
const TOTAL = 100_000;
const BATCH = 500;
const outputDir = './datos';

const ciudades  = ['Aguascalientes','Guadalajara','CDMX','Monterrey','Puebla'];
const ocups     = ['Estudiante','Ingeniero','Médico','Docente','Comerciante'];
const niveles   = ['Preparatoria','Licenciatura','Maestría','Doctorado'];
const pagos     = ['Efectivo','Tarjeta','Transferencia','OXXO'];
const estatuses = ['Completada','Pendiente','Cancelada'];
const sensTypes = ['Temperatura','Humedad','Presión','CO2','Luz'];
const locs      = ['Sala','Cocina','Oficina','Almacén','Exterior'];
const estados   = ['Normal','Alerta','Crítico'];
const servicios = ['AUTH','DATABASE','API','KAFKA','SPARK','STORAGE','NETWORK'];

function ensureOutputDir() {
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (text.includes(',') || text.includes('"') || text.includes('\n')) {
    return '"' + text.replace(/"/g, '""') + '"';
  }
  return text;
}

function persona(id) {
  return {
    id_persona:      id,
    nombre:          faker.person.firstName(),
    apellido:        faker.person.lastName(),
    edad:            faker.number.int({ min: 18, max: 65 }),
    genero:          faker.helpers.arrayElement(['M', 'F']),
    ciudad:          faker.helpers.arrayElement(ciudades),
    estado:          faker.helpers.arrayElement(estados),
    ocupacion:       faker.helpers.arrayElement(ocups),
    nivel_estudios:  faker.helpers.arrayElement(niveles),
    ingreso_mensual: parseFloat(faker.finance.amount({ min: 3000, max: 30000, dec: 2 })),
    activo:          faker.datatype.boolean(),
    fecha_registro:  faker.date.between({ from:'2024-01-01', to:'2025-06-01' }).toISOString().split('T')[0]
  };
}

function venta(id) {
  return {
    id_venta:       id,
    id_persona:     faker.number.int({ min: 1, max: 10000 }),
    producto:       faker.commerce.productName(),
    categoria:      faker.commerce.department(),
    cantidad:       faker.number.int({ min: 1, max: 20 }),
    precio_unit:    parseFloat(faker.commerce.price({ min: 10, max: 5000 })),
    total:          parseFloat(faker.commerce.price({ min: 10, max: 50000 })),
    fecha_venta:    faker.date.between({ from:'2024-01-01', to:'2025-06-01' }).toISOString().split('T')[0],
    metodo_pago:    faker.helpers.arrayElement(pagos),
    estatus:        faker.helpers.arrayElement(estatuses),
    canal:          faker.helpers.arrayElement(['Online', 'Tienda', 'App', 'Televenta']),
    cliente_vip:    faker.datatype.boolean()
  };
}

function sensor(id) {
  return {
    id_sensor:   id,
    tipo:        faker.helpers.arrayElement(sensTypes),
    ubicacion:   faker.helpers.arrayElement(locs),
    valor:       parseFloat(faker.number.float({ min: 0, max: 100, fractionDigits: 2 })),
    unidad:      faker.helpers.arrayElement(['°C','%','hPa','ppm','lux']),
    temperatura: parseFloat(faker.number.float({ min: 15, max: 40, fractionDigits: 1 })),
    humedad:     parseFloat(faker.number.float({ min: 20, max: 90, fractionDigits: 1 })),
    estado:      faker.helpers.arrayElement(estados),
    timestamp:   faker.date.recent({ days: 30 }).toISOString(),
    bateria:     faker.number.int({ min: 5, max: 100 }),
    alerta:      faker.datatype.boolean()
  };
}

function logSistema(id) {
  const niveles = ['INFO', 'WARNING', 'ERROR', 'DEBUG', 'CRITICAL'];
  const modulos = ['AUTH', 'DATABASE', 'API', 'KAFKA', 'SPARK', 'STORAGE', 'NETWORK'];
  return {
    id_log:        id,
    nivel:         faker.helpers.arrayElement(niveles),
    modulo:        faker.helpers.arrayElement(modulos),
    servicio:      faker.helpers.arrayElement(servicios),
    codigo_error:  faker.helpers.arrayElement(['200', '300', '400', '500', '503']),
    mensaje:       faker.lorem.sentence(),
    usuario:       faker.internet.userName(),
    ip:            faker.internet.ipv4(),
    timestamp:     faker.date.recent({ days: 7 }).toISOString(),
    duracion_ms:   faker.number.int({ min: 10, max: 5000 })
  };
}

function transaccion(id) {
  const tipos = ['Depósito','Retiro','Transferencia','Pago de servicios','Inversión'];
  return {
    id_transaccion: id,
    id_persona:     faker.number.int({ min: 1, max: 10000 }),
    tipo:           faker.helpers.arrayElement(tipos),
    monto:          parseFloat(faker.finance.amount({ min: 100, max: 50000, dec: 2 })),
    moneda:         'MXN',
    estado:         faker.helpers.arrayElement(estatuses),
    banco_origen:   faker.company.name(),
    banco_destino:  faker.company.name(),
    concepto:       faker.lorem.words(3),
    fecha_hora:     faker.date.recent({ days: 30 }).toISOString(),
    comision:       parseFloat(faker.finance.amount({ min: 0, max: 500, dec: 2 })),
    aprobada:       faker.datatype.boolean()
  };
}

function buildCsvLine(fields) {
  return fields.map(csvEscape).join(',') + '\n';
}

function safeSql(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number' || typeof value === 'boolean') return value.toString();
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function run() {
  ensureOutputDir();
  await producer.connect();

  const jsonStreams = {
    personas: fs.createWriteStream(`${outputDir}/personas.json`),
    ventas: fs.createWriteStream(`${outputDir}/ventas.json`),
    'logs-sistema': fs.createWriteStream(`${outputDir}/logs-sistema.json`),
    sensores: fs.createWriteStream(`${outputDir}/sensores.json`),
    transacciones: fs.createWriteStream(`${outputDir}/transacciones.json`)
  };

  const firstJson = {
    personas: true,
    ventas: true,
    'logs-sistema': true,
    sensores: true,
    transacciones: true
  };

  const csvStreams = {
    personas: fs.createWriteStream(`${outputDir}/personas.csv`),
    ventas: fs.createWriteStream(`${outputDir}/ventas.csv`),
    'logs-sistema': fs.createWriteStream(`${outputDir}/logs-sistema.csv`),
    sensores: fs.createWriteStream(`${outputDir}/sensores.csv`),
    transacciones: fs.createWriteStream(`${outputDir}/transacciones.csv`)
  };

  const sqlStreams = {
    personas: fs.createWriteStream(`${outputDir}/personas.sql`),
    ventas: fs.createWriteStream(`${outputDir}/ventas.sql`),
    'logs-sistema': fs.createWriteStream(`${outputDir}/logs-sistema.sql`),
    sensores: fs.createWriteStream(`${outputDir}/sensores.sql`),
    transacciones: fs.createWriteStream(`${outputDir}/transacciones.sql`)
  };

  csvStreams.personas.write('id_persona,nombre,apellido,edad,genero,ciudad,estado,ocupacion,nivel_estudios,ingreso_mensual,activo,fecha_registro\n');
  csvStreams.ventas.write('id_venta,id_persona,producto,categoria,cantidad,precio_unit,total,fecha_venta,metodo_pago,estatus,canal,cliente_vip\n');
  csvStreams['logs-sistema'].write('id_log,nivel,modulo,servicio,codigo_error,mensaje,usuario,ip,timestamp,duracion_ms\n');
  csvStreams.sensores.write('id_sensor,tipo,ubicacion,valor,unidad,temperatura,humedad,estado,timestamp,bateria,alerta\n');
  csvStreams.transacciones.write('id_transaccion,id_persona,tipo,monto,moneda,estado,banco_origen,banco_destino,concepto,fecha_hora,comision,aprobada\n');

  sqlStreams.personas.write(`CREATE TABLE IF NOT EXISTS personas (
  id_persona INT, nombre VARCHAR(50), apellido VARCHAR(50), edad INT,
  genero CHAR(1), ciudad VARCHAR(50), estado VARCHAR(50), ocupacion VARCHAR(50),
  nivel_estudios VARCHAR(50), ingreso_mensual DECIMAL(10,2), activo BOOLEAN, fecha_registro DATE
);\n\n`);

  sqlStreams.ventas.write(`CREATE TABLE IF NOT EXISTS ventas (
  id_venta INT, id_persona INT, producto VARCHAR(100), categoria VARCHAR(50),
  cantidad INT, precio_unit DECIMAL(10,2), total DECIMAL(10,2),
  fecha_venta DATE, metodo_pago VARCHAR(30), estatus VARCHAR(20), canal VARCHAR(30), cliente_vip BOOLEAN
);\n\n`);

  sqlStreams['logs-sistema'].write(`CREATE TABLE IF NOT EXISTS logs_sistema (
  id_log INT, nivel VARCHAR(20), modulo VARCHAR(50), servicio VARCHAR(50),
  codigo_error VARCHAR(10), mensaje TEXT, usuario VARCHAR(50), ip VARCHAR(20),
  timestamp TIMESTAMP, duracion_ms INT
);\n\n`);

  sqlStreams.sensores.write(`CREATE TABLE IF NOT EXISTS sensores (
  id_sensor INT, tipo VARCHAR(30), ubicacion VARCHAR(30), valor DECIMAL(8,2),
  unidad VARCHAR(10), temperatura DECIMAL(5,1), humedad DECIMAL(5,1),
  estado VARCHAR(20), timestamp TIMESTAMP, bateria INT, alerta BOOLEAN
);\n\n`);

  sqlStreams.transacciones.write(`CREATE TABLE IF NOT EXISTS transacciones (
  id_transaccion INT, id_persona INT, tipo VARCHAR(50), monto DECIMAL(12,2),
  moneda VARCHAR(10), estado VARCHAR(20), banco_origen VARCHAR(100),
  banco_destino VARCHAR(100), concepto VARCHAR(200), fecha_hora TIMESTAMP,
  comision DECIMAL(10,2), aprobada BOOLEAN
);\n\n`);

  for (const key of Object.keys(jsonStreams)) {
    jsonStreams[key].write('[\n');
  }

  console.log('✅ Productor conectado y archivos de salida preparados');
  const start = Date.now();

  for (let i = 0; i < TOTAL; i += BATCH) {
    const mp = [], mv = [], ml = [], ms = [], mt = [];

    for (let j = i + 1; j <= Math.min(i + BATCH, TOTAL); j++) {
      const mod = j % 5;
      if (mod === 0) {
        const record = persona(j);
        mp.push({ key: String(j), value: JSON.stringify(record) });
        writeRecord('personas', record, jsonStreams, csvStreams, sqlStreams, firstJson);
      } else if (mod === 1) {
        const record = venta(j);
        mv.push({ key: String(j), value: JSON.stringify(record) });
        writeRecord('ventas', record, jsonStreams, csvStreams, sqlStreams, firstJson);
      } else if (mod === 2) {
        const record = logSistema(j);
        ml.push({ key: String(j), value: JSON.stringify(record) });
        writeRecord('logs-sistema', record, jsonStreams, csvStreams, sqlStreams, firstJson);
      } else if (mod === 3) {
        const record = sensor(j);
        ms.push({ key: String(j), value: JSON.stringify(record) });
        writeRecord('sensores', record, jsonStreams, csvStreams, sqlStreams, firstJson);
      } else {
        const record = transaccion(j);
        mt.push({ key: String(j), value: JSON.stringify(record) });
        writeRecord('transacciones', record, jsonStreams, csvStreams, sqlStreams, firstJson);
      }
    }

    if (mp.length) await producer.send({ topic:'personas', messages: mp });
    if (mv.length) await producer.send({ topic:'ventas', messages: mv });
    if (ml.length) await producer.send({ topic:'logs-sistema', messages: ml });
    if (ms.length) await producer.send({ topic:'sensores', messages: ms });
    if (mt.length) await producer.send({ topic:'transacciones', messages: mt });

    const total = Math.min(i + BATCH, TOTAL);
    if (total % 10_000 === 0) console.log(`📤 ${total.toLocaleString()} / ${TOTAL.toLocaleString()}`);
  }

  for (const key of Object.keys(jsonStreams)) {
    jsonStreams[key].write('\n]\n');
    jsonStreams[key].end();
    csvStreams[key].end();
    sqlStreams[key].end();
  }

  await producer.disconnect();
  const elapsed = ((Date.now() - start) / 1000).toFixed(2);
  console.log(`\n✅ ${TOTAL.toLocaleString()} mensajes enviados y datos generados en ${elapsed}s`);
  console.log(`   Archivos en ${outputDir}/*.json, *.csv, *.sql`);
}

function writeRecord(topic, data, jsonStreams, csvStreams, sqlStreams, firstJson) {
  const jsonStream = jsonStreams[topic];
  if (!firstJson[topic]) jsonStream.write(',\n');
  firstJson[topic] = false;
  jsonStream.write(JSON.stringify(data, null, 2));

  if (topic === 'personas') {
    csvStreams.personas.write(buildCsvLine([
      data.id_persona, data.nombre, data.apellido, data.edad, data.genero,
      data.ciudad, data.estado, data.ocupacion, data.nivel_estudios,
      data.ingreso_mensual, data.activo, data.fecha_registro
    ]));
    sqlStreams.personas.write(`INSERT INTO personas VALUES (${safeSql(data.id_persona)},${safeSql(data.nombre)},${safeSql(data.apellido)},${safeSql(data.edad)},${safeSql(data.genero)},${safeSql(data.ciudad)},${safeSql(data.estado)},${safeSql(data.ocupacion)},${safeSql(data.nivel_estudios)},${safeSql(data.ingreso_mensual)},${safeSql(data.activo)},${safeSql(data.fecha_registro)});\n`);
  } else if (topic === 'ventas') {
    csvStreams.ventas.write(buildCsvLine([
      data.id_venta, data.id_persona, data.producto, data.categoria, data.cantidad,
      data.precio_unit, data.total, data.fecha_venta, data.metodo_pago,
      data.estatus, data.canal, data.cliente_vip
    ]));
    sqlStreams.ventas.write(`INSERT INTO ventas VALUES (${safeSql(data.id_venta)},${safeSql(data.id_persona)},${safeSql(data.producto)},${safeSql(data.categoria)},${safeSql(data.cantidad)},${safeSql(data.precio_unit)},${safeSql(data.total)},${safeSql(data.fecha_venta)},${safeSql(data.metodo_pago)},${safeSql(data.estatus)},${safeSql(data.canal)},${safeSql(data.cliente_vip)});\n`);
  } else if (topic === 'logs-sistema') {
    csvStreams['logs-sistema'].write(buildCsvLine([
      data.id_log, data.nivel, data.modulo, data.servicio, data.codigo_error,
      data.mensaje, data.usuario, data.ip, data.timestamp, data.duracion_ms
    ]));
    sqlStreams['logs-sistema'].write(`INSERT INTO logs_sistema VALUES (${safeSql(data.id_log)},${safeSql(data.nivel)},${safeSql(data.modulo)},${safeSql(data.servicio)},${safeSql(data.codigo_error)},${safeSql(data.mensaje)},${safeSql(data.usuario)},${safeSql(data.ip)},${safeSql(data.timestamp)},${safeSql(data.duracion_ms)});\n`);
  } else if (topic === 'sensores') {
    csvStreams.sensores.write(buildCsvLine([
      data.id_sensor, data.tipo, data.ubicacion, data.valor, data.unidad,
      data.temperatura, data.humedad, data.estado, data.timestamp, data.bateria, data.alerta
    ]));
    sqlStreams.sensores.write(`INSERT INTO sensores VALUES (${safeSql(data.id_sensor)},${safeSql(data.tipo)},${safeSql(data.ubicacion)},${safeSql(data.valor)},${safeSql(data.unidad)},${safeSql(data.temperatura)},${safeSql(data.humedad)},${safeSql(data.estado)},${safeSql(data.timestamp)},${safeSql(data.bateria)},${safeSql(data.alerta)});\n`);
  } else if (topic === 'transacciones') {
    csvStreams.transacciones.write(buildCsvLine([
      data.id_transaccion, data.id_persona, data.tipo, data.monto, data.moneda,
      data.estado, data.banco_origen, data.banco_destino, data.concepto,
      data.fecha_hora, data.comision, data.aprobada
    ]));
    sqlStreams.transacciones.write(`INSERT INTO transacciones VALUES (${safeSql(data.id_transaccion)},${safeSql(data.id_persona)},${safeSql(data.tipo)},${safeSql(data.monto)},${safeSql(data.moneda)},${safeSql(data.estado)},${safeSql(data.banco_origen)},${safeSql(data.banco_destino)},${safeSql(data.concepto)},${safeSql(data.fecha_hora)},${safeSql(data.comision)},${safeSql(data.aprobada)});\n`);
  }
}

run().catch(e => { console.error('❌', e); process.exit(1); });