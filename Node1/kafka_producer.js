const { Kafka } = require('kafkajs');
const { faker } = require('@faker-js/faker');
const { version } = require('react');

// Node's connection
const kafka = new Kafka({
  clientId: 'master-producer',
  brokers: ['192.168.1.101:9092', '192.168.1.102:9092', '192.168.1.103:9092',]
});

const producer = kafka.producer();
const topics = ['personas', 'ventas', 'logs-sistema', 'sensores', 'transacciones'];

// Message's generating functions
function generatePersona(){
  return {
    id_persona: faker.string.uuid(),
    nombre: faker.person.firstName(),
    apellido: faker.person.lastName(),
    email: faker.internet.email(),
    usuario: faker.internet.username(),
    edad: faker.number.int({min: 18, max: 80}),
    genero: faker.person.sex(),
    ciudad: faker.location.city(),
    ingreso_mensual: faker.finance.amount({min: 5000, max: 50000, dec: 2}),
    fecha_registro: faker.date.past().toISOString()
  };
}

function generateVenta(){
  const cant = faker.number.int({min: 1, max: 5});
  const precio = parseFloat(faker.commerce.price({min: 100, max: 2000}));
  const sub = cant * precio;

  return {
    id_venta: faker.string.uuid(),
    fecha: faker.date.recent().toISOString(),
    cliente: faker.person.fullName(),
    producto: faker.commerce.productName(),
    categoria: faker.commerce.department(),
    cantidad: cant,
    precio_unitario: precio,
    subtotal: sub,
    impuesto: sub * 0.16,
    total: sub * 1.16,
    metodo_pago: faker.finance.transactionType(),
    estatus: faker.helpers.arrayElement(['Completada', 'Pendiente', 'Cancelada'])
  };
}

function generateTransaccion(){
  return {
    id_trx: faker.string.uuid(),
    fecha: faker.date.recent().toISOString(),
    cuenta_origen: faker.finance.accountNumber(),
    cuenta_destino: faker.finance.accountNumber(),
    banco: faker.company.name(),
    monto: faker.finance.amount({min: 100, max: 10000, dec: 2}),
    divisa: faker.helpers.arrayElement(['Transferencia', 'Pago', 'Retiro']),
    dispositivo: faker.internet.userAgent(),
    ubicacion_ip: faker.internet.ipv4(),
    aprobada: faker.datatype.boolean()
  };
}

function generateLog(){
  return {
    id_log: faker.string.uuid(),
    fecha_hora: faker.date.recent().toISOString(),
    nivel: faker.helpers.arrayElement(['INFO', 'WARN', 'ERROR', 'DEBUG', 'FATAL']),
    modulo: faker.hacker.noun(),
    mensaje: faker.hacker.phrase(),
    ip_origen: faker.internet.ipv4(),
    usuario_sesion: faker.internet.username(),
    codigo_estado: faker.helpers.arrayElement([200, 201, 400, 401, 403, 404, 500, 502]),
    tiempo_respuesta_ms: faker.number.int({min: 10, max: 5000}),
    entorno: faker.helpers.arrayElement(['Desarrollo', 'Pruebas', 'Producción'])
  };
}

function generateSensor(){
  const tipo = faker.helpers.arrayElement(['Temperatura', 'Humedad', 'Presión', 'Luz', 'Movimiento']);
  let unidad = '';
  let valorMin = 0, valorMax = 100;

  switch(tipo){
    case 'Temperatura': unidad = '°C'; valorMin = -10; valorMax = 45; break;
    case 'Humedad': unidad = '%'; valorMin = 20; valorMax = 95; break;
    case 'Presión': unidad = 'hPa'; valorMin = 900; valorMax = 1100; break;
    case 'Luz': unidad = 'Lux'; valorMin = 0; valorMax = 1000; break;
    case 'Movimiento': unidad = 'Boolean'; valorMin = 0; valorMax = 1; break;
  }

  return {
    id_lecturas: faker.string.uuid(),
    mac_address_sensor: faker.internet.mac(),
    fecha_hora: faker.date.recent().toISOString(),
    tipo_sensor: tipo,
    valor_registrado: faker.number.float({min: valorMin, max: valorMax, fractionDigits: 2}),
    unidad_medida: unidad,
    ubicacion: faker.location.buildingNumber() + ' ' + faker.location.street(),
    nivel_bateria: faker.number.int({min: 0, max: 100}),
    alerta_activa: faker.datatype.boolean(),
    version_firmware: faker.system.semver()
  };
}

// Principal function
async function run(){
  await producer.connect();
  console.log("Producer connected. Starting message generation...");
  
  const TOTAL_MESSAGES = 100000;

  for(let i=1; i<=TOTAL_MESSAGES; i++){
    const actualTopic = topics[Math.floor(Math.random() * topics.length)];

    let objData;
    switch(actualTopic) {
      case 'personas': objData = generatePersona(); break;
      case 'ventas': objData = generateVenta(); break;
      case 'transacciones': objData = generateTransaccion(); break;
      case 'logs-sistema': objData = generateLog(); break;
      case 'sensores': objData = generateSensor(); break;
      default: objData = generateLog();
    }

    const values = Object.values(objData);
    const messagePayload = {
      json: JSON.stringify(objData),
      csv: values.map(v => `"${v}"`).join(','),
      sql: `INSERT INTO ${actualTopic.replace('-', '-')} VALUES (${values.map(v => `'${v}'`).join(',')});`
    };

    await producer.send({
      topic: actualTopic,
      messages: [{value: JSON.stringify(messagePayload)}],
    });

    if(i%100000 === 0){
      console.log(`${i} / ${TOTAL_MESSAGES} messages sent and ditributed...`)
    }
  }

  console.log("Sending 100,000 messages completed");
  await producer.disconnect();
}

run().catch(console.error);

