const fs = require("fs");
const https = require("https");
const url = require("url");

r = require("rethinkdb");
const mysql = require('mysql');
const MySQLEvents = require('@rodrigogs/mysql-events');

const ora = require('ora'); // cool spinner
const spinner = ora({
  text: 'Waiting for database events... ',
  color: 'blue',
  spinner: 'dots2'
});

const config = require("./config.json");

/**
 * Read SSL Certificates
 */
const privateKey = fs.readFileSync(config.app.ssl.privateKey, "utf8");
const certificate = fs.readFileSync(config.app.ssl.certificate, "utf8");
const credentials = { key: privateKey, cert: certificate };

/**
 * @type {Server}
 */
const httpsServer = https.createServer(credentials);
httpsServer.listen(8443);

const WebSocket = require("ws");
const WebSocketServer = require("ws").Server;

const wss_room_stats = new WebSocketServer({ noServer: true });
const wss_location_status = new WebSocketServer({ noServer: true });
const wss_motion_detection = new WebSocketServer({ noServer: true });

wss_room_stats.on('connection', (wsconn) => {
  // const user = conn.upgradeReq.user;
  wsconn.send('Welcome wss_room_stats!'); // + user.name

  wsconn.on('message', (data) => {
    console.log('message', data);
  });
});

wss_location_status.on('connection', (wsconn) => {
  wsconn.send('Welcome wss_location_status!'); // + user.name

  wsconn.on('message', (data) => {
    console.log('message', data);
  });
});

wss_motion_detection.on('connection', (wsconn) => {
  wsconn.send('Welcome wss_location_status!'); // + user.name

  wsconn.on('message', (data) => {
    console.log('message', data);
  });
});

httpsServer.on("upgrade", function upgrade(request, socket, head) {
  const pathname = url.parse(request.url).pathname;

  if (pathname === "/room_stats") {
    wss_room_stats.handleUpgrade(request, socket, head, function done(ws) {
      wss_room_stats.emit("connection", ws, request);
    });
  } else if (pathname === "/location_stats") {
    wss_location_status.handleUpgrade(request, socket, head, function done(ws) {
      wss_location_status.emit("connection", ws, request);
    });
  } else if (pathname === "/motion_detection") {
    wss_motion_detection.handleUpgrade(request, socket, head, function done(ws) {
      wss_motion_detection.emit("connection", ws, request);
    });
  } else {
    socket.destroy();
  }
});

r.connect({ host: config.rethinkdb.host, port: config.rethinkdb.port },
  function(err, conn) {
  if (err) throw err;
    r.db("rpi")
      .table("temp_hum_log")
      .changes()
      .run(conn, function(err, cursor) {
        if (err) throw err;
        cursor.each(function(err, row) {
          if (err) throw err;
          wss_room_stats.clients.forEach(function each(client) {
            if (client.readyState === WebSocket.OPEN) {
              let data = JSON.stringify({ room_stats: row }, null, 2);
              client.send(data);
            }
          });
        });
      });


    r.db("rpi")
      .table("motion_detection")
      .changes()
      .run(conn, function(err, cursor) {
        if (err) throw err;
        cursor.each(function(err, row) {
          if (err) throw err;
          wss_motion_detection.clients.forEach(function each(client) {
            if (client.readyState === WebSocket.OPEN) {
              let data = JSON.stringify({ motion_detection: row }, null, 2);
              client.send(data);
            }
          });
        });
      });
});


const db_program = async () => {
  const connection = mysql.createConnection({
    host: config.mariadb.host,
    user: config.mariadb.user,
    password: config.mariadb.password
  });

  const instance = new MySQLEvents(connection, {
    startAtEnd: true // to record only the new binary logs, if set to false or you didn'y provide it all the events will be console.logged after you start the app
  });

  await instance.start();

  instance.addTrigger({
    name: 'monitoring all statments',
    expression: 'iot.location_stats',
    statement: MySQLEvents.STATEMENTS.UPDATED,
    onEvent: e => {
      spinner.start();
      wss_location_status.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
          let data = e.affectedRows[0].after;
          client.send(JSON.stringify(data));
        }
      });
    }
  });

  instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
  instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
};

db_program()
  .then(spinner.start.bind(spinner))
  .catch(console.error);

console.log("server waiting for connection on port: 8443");
