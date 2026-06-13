const { Kafka } = require('kafkajs');
const fs = require('fs');

const kafka = new Kafka({
  clientId: 'consumidor',
  brokers: ['192.168.1.101:9092','192.168.1.102:9092','192.168.1.103:9092']
});

const consumer = kafka.consumer({ groupId: 'grupo-almacenamiento' });

if (!fs.existsSync('./datos')) fs.mkdirSync('./datos');

const csvPersonas      = fs.createWriteStream('./datos/personas.csv');
const csvVentas        = fs.createWriteStream('./datos/ventas.csv');
const csvLogs          = fs.createWriteStream('./datos/logs-sistema.csv');
const csvSensores      = fs.createWriteStream('./datos/sensores.csv');
const csvTransacciones = fs.createWriteStream('./datos/transacciones.csv');

const sqlPersonas      = fs.createWriteStream('./datos/personas.sql');
const sqlVentas        = fs.createWriteStream('./datos/ventas.sql');
const sqlLogs          = fs.createWriteStream('./datos/logs-sistema.sql');
const sqlSensores      = fs.createWriteStream('./datos/sensores.sql');
const sqlTransacciones = fs.createWriteStream('./datos/transacciones.sql');

const jsonPersonas      = [];
const jsonVentas        = [];
const jsonLogs          = [];
const jsonSensores      = [];
const jsonTransacciones = [];

csvPersonas.write('id_persona,nombre,apellido,edad,genero,ciudad,estado,ocupacion,nivel_estudios,ingreso_mensual,activo,fecha_registro\n');
csvVentas.write('id_venta,id_persona,producto,categoria,cantidad,precio_unit,total,fecha_venta,metodo_pago,estatus,canal,cliente_vip\n');
csvLogs.write('id_log,nivel,modulo,servicio,codigo_error,mensaje,usuario,ip,timestamp,duracion_ms\n');
csvSensores.write('id_sensor,tipo,ubicacion,valor,unidad,temperatura,humedad,estado,timestamp,bateria,alerta\n');
csvTransacciones.write('id_transaccion,id_persona,tipo,monto,moneda,estado,banco_origen,banco_destino,concepto,fecha_hora,comision,aprobada\n');

sqlPersonas.write(`CREATE TABLE IF NOT EXISTS personas (\n  id_persona INT, nombre VARCHAR(50), apellido VARCHAR(50), edad INT,\n  genero CHAR(1), ciudad VARCHAR(50), estado VARCHAR(50), ocupacion VARCHAR(50),\n  nivel_estudios VARCHAR(50), ingreso_mensual DECIMAL(10,2), activo BOOLEAN, fecha_registro DATE\n);\n\n`);
sqlVentas.write(`CREATE TABLE IF NOT EXISTS ventas (\n  id_venta INT, id_persona INT, producto VARCHAR(100), categoria VARCHAR(50),\n  cantidad INT, precio_unit DECIMAL(10,2), total DECIMAL(10,2),\n  fecha_venta DATE, metodo_pago VARCHAR(30), estatus VARCHAR(20), canal VARCHAR(30), cliente_vip BOOLEAN\n);\n\n`);
sqlLogs.write(`CREATE TABLE IF NOT EXISTS logs_sistema (\n  id_log INT, nivel VARCHAR(20), modulo VARCHAR(50), servicio VARCHAR(50),\n  codigo_error VARCHAR(10), mensaje TEXT, usuario VARCHAR(50), ip VARCHAR(20),\n  timestamp TIMESTAMP, duracion_ms INT\n);\n\n`);
sqlSensores.write(`CREATE TABLE IF NOT EXISTS sensores (\n  id_sensor INT, tipo VARCHAR(30), ubicacion VARCHAR(30), valor DECIMAL(8,2),\n  unidad VARCHAR(10), temperatura DECIMAL(5,1), humedad DECIMAL(5,1),\n  estado VARCHAR(20), timestamp TIMESTAMP, bateria INT, alerta BOOLEAN\n);\n\n`);
sqlTransacciones.write(`CREATE TABLE IF NOT EXISTS transacciones (\n  id_transaccion INT, id_persona INT, tipo VARCHAR(50), monto DECIMAL(12,2),\n  moneda VARCHAR(10), estado VARCHAR(20), banco_origen VARCHAR(100),\n  banco_destino VARCHAR(100), concepto VARCHAR(200), fecha_hora TIMESTAMP,\n  comision DECIMAL(10,2), aprobada BOOLEAN\n);\n\n`);

const conteo = { personas: 0, ventas: 0, 'logs-sistema': 0, sensores: 0, transacciones: 0 };
const TOTAL = 100_000;

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (/[",\n]/.test(text)) {
    return '"' + text.replace(/"/g, '""') + '"';
  }
  return text;
}

function safeSql(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number' || typeof value === 'boolean') return value.toString();
  return `'${String(value).replace(/'/g, "''")}'`;
}

function buildCsvLine(values) {
  return values.map(csvEscape).join(',') + '\n';
}

function guardarJSON() {
  fs.writeFileSync('./datos/personas.json', JSON.stringify(jsonPersonas, null, 2));
  fs.writeFileSync('./datos/ventas.json', JSON.stringify(jsonVentas, null, 2));
  fs.writeFileSync('./datos/logs-sistema.json', JSON.stringify(jsonLogs, null, 2));
  fs.writeFileSync('./datos/sensores.json', JSON.stringify(jsonSensores, null, 2));
  fs.writeFileSync('./datos/transacciones.json', JSON.stringify(jsonTransacciones, null, 2));
}

async function run() {
  await consumer.connect();
  console.log('✅ Consumidor conectado — esperando mensajes...\n');

  await consumer.subscribe({
    topics: ['personas','ventas','logs-sistema','sensores','transacciones'],
    fromBeginning: true
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const d = JSON.parse(message.value.toString());

      if (topic === 'personas') {
        csvPersonas.write(buildCsvLine([d.id_persona, d.nombre, d.apellido, d.edad, d.genero, d.ciudad, d.estado, d.ocupacion, d.nivel_estudios, d.ingreso_mensual, d.activo, d.fecha_registro]));
        sqlPersonas.write(`INSERT INTO personas VALUES (${safeSql(d.id_persona)},${safeSql(d.nombre)},${safeSql(d.apellido)},${safeSql(d.edad)},${safeSql(d.genero)},${safeSql(d.ciudad)},${safeSql(d.estado)},${safeSql(d.ocupacion)},${safeSql(d.nivel_estudios)},${safeSql(d.ingreso_mensual)},${safeSql(d.activo)},${safeSql(d.fecha_registro)});\n`);
        jsonPersonas.push(d);
      } else if (topic === 'ventas') {
        csvVentas.write(buildCsvLine([d.id_venta, d.id_persona, d.producto, d.categoria, d.cantidad, d.precio_unit, d.total, d.fecha_venta, d.metodo_pago, d.estatus, d.canal, d.cliente_vip]));
        sqlVentas.write(`INSERT INTO ventas VALUES (${safeSql(d.id_venta)},${safeSql(d.id_persona)},${safeSql(d.producto)},${safeSql(d.categoria)},${safeSql(d.cantidad)},${safeSql(d.precio_unit)},${safeSql(d.total)},${safeSql(d.fecha_venta)},${safeSql(d.metodo_pago)},${safeSql(d.estatus)},${safeSql(d.canal)},${safeSql(d.cliente_vip)});\n`);
        jsonVentas.push(d);
      } else if (topic === 'logs-sistema') {
        csvLogs.write(buildCsvLine([d.id_log, d.nivel, d.modulo, d.servicio, d.codigo_error, d.mensaje, d.usuario, d.ip, d.timestamp, d.duracion_ms]));
        sqlLogs.write(`INSERT INTO logs_sistema VALUES (${safeSql(d.id_log)},${safeSql(d.nivel)},${safeSql(d.modulo)},${safeSql(d.servicio)},${safeSql(d.codigo_error)},${safeSql(d.mensaje)},${safeSql(d.usuario)},${safeSql(d.ip)},${safeSql(d.timestamp)},${safeSql(d.duracion_ms)});\n`);
        jsonLogs.push(d);
      } else if (topic === 'sensores') {
        csvSensores.write(buildCsvLine([d.id_sensor, d.tipo, d.ubicacion, d.valor, d.unidad, d.temperatura, d.humedad, d.estado, d.timestamp, d.bateria, d.alerta]));
        sqlSensores.write(`INSERT INTO sensores VALUES (${safeSql(d.id_sensor)},${safeSql(d.tipo)},${safeSql(d.ubicacion)},${safeSql(d.valor)},${safeSql(d.unidad)},${safeSql(d.temperatura)},${safeSql(d.humedad)},${safeSql(d.estado)},${safeSql(d.timestamp)},${safeSql(d.bateria)},${safeSql(d.alerta)});\n`);
        jsonSensores.push(d);
      } else if (topic === 'transacciones') {
        csvTransacciones.write(buildCsvLine([d.id_transaccion, d.id_persona, d.tipo, d.monto, d.moneda, d.estado, d.banco_origen, d.banco_destino, d.concepto, d.fecha_hora, d.comision, d.aprobada]));
        sqlTransacciones.write(`INSERT INTO transacciones VALUES (${safeSql(d.id_transaccion)},${safeSql(d.id_persona)},${safeSql(d.tipo)},${safeSql(d.monto)},${safeSql(d.moneda)},${safeSql(d.estado)},${safeSql(d.banco_origen)},${safeSql(d.banco_destino)},${safeSql(d.concepto)},${safeSql(d.fecha_hora)},${safeSql(d.comision)},${safeSql(d.aprobada)});\n`);
        jsonTransacciones.push(d);
      }

      conteo[topic] += 1;
      const total = Object.values(conteo).reduce((sum, value) => sum + value, 0);
      if (total % 10_000 === 0) {
        console.log(`📥 ${total.toLocaleString()} recibidos | personas: ${conteo.personas} | ventas: ${conteo.ventas} | logs: ${conteo['logs-sistema']} | sensores: ${conteo.sensores} | transacciones: ${conteo.transacciones}`);
      }

      if (total >= TOTAL) {
        guardarJSON();
        console.log('\n✅ Archivos guardados en ./datos/');
        console.log('   personas.csv  personas.json  personas.sql');
        console.log('   ventas.csv    ventas.json    ventas.sql');
        console.log('   logs-sistema.csv  logs-sistema.json  logs-sistema.sql');
        console.log('   sensores.csv  sensores.json  sensores.sql');
        console.log('   transacciones.csv  transacciones.json  transacciones.sql');
        await consumer.disconnect();
        process.exit(0);
      }
    }
  });
}

run().catch(e => { console.error('❌', e); process.exit(1); });
